# protobase-changes

Write-efficient Postgres change tracking for live queries, replicas, webhooks and push notifications, with an Iceberg archive for history and analytics.

## Architecture

Postgres writes every committed change to its write-ahead log (WAL) before anything else. Logical replication turns that log into row changes: which table, which key, and the old and new values. A reader follows it through a replication slot, which marks how far the reader got; Postgres keeps the WAL from that point on until the reader confirms it. Reading changes this way is called change data capture (CDC), and it needs no extra writes: no triggers, no outbox table.

protobase-changes is that reader. It runs next to the app and the relay:

```mermaid
flowchart LR
  App["Protobase app<br/>decides access, issues tokens"]
  PG[("Postgres")]
  Changes["protobase-changes<br/>reads, matches, keeps cursors"]
  Relay["protobase-realtime<br/>holds user sockets"]
  Users["Browsers and apps"]
  Pull["Replicas and ETL"]
  Push["Webhooks, functions, push sender"]
  Archive[("Iceberg archive<br/>object storage")]

  App -- writes --> PG
  PG -- logical replication --> Changes
  App -- live tokens --> Users
  Users -- one socket --> Relay
  Relay -- subscriptions --> Changes
  Changes -- matched changes --> Relay
  Changes -- pull by cursor --> Pull
  Changes -- batches --> Push
  Changes --> Archive
```

- **The app (Protobase)** serves data and issues every token, with its access rules inside. It holds no sockets and no delivery state, so it can sleep.
- **protobase-changes** holds one replication slot per app database, matches conditions against each changed row and keeps cursors. Access is decided in the app, never here.
- **[protobase-realtime](https://github.com/PrototeamHQ/protobase-realtime)**, the relay, holds one connection per user for live queries, notifications, events and presence.

Replicas connect to protobase-changes directly, not through the relay: a backfill would slow every user's live updates, and the relay keeps nothing to replay.

## Design choices

One service reads every change once and serves every way changes leave the database. Each choice below solves a problem that usually needs its own tool.

- **Read the WAL instead of triggers.** Postgres already logs every change, so one replication slot per database replaces outbox tables, triggers and a job row per destination. A write stays one write.
- **The app writes the actor into the WAL.** Who made a change travels as one small logical message per transaction, in the same round trip as the write. No extra table.
- **The app decides access, the service stays simple.** The app turns the user's filter plus the resource's access rule into plain conditions on one row. Subqueries such as "channels I belong to" are resolved when the token is issued, and the app refreshes the subscription when access changes. The service only matches conditions; it never evaluates access rules.
- **Live tokens come with the data.** A list request with `Prefer: live` returns an encrypted token in a header: the conditions, the user and session, and the cursor of the snapshot that was read. Nothing slips between the read and the subscription, and the client can read nothing in the token. Caching stays with the endpoint.
- **One socket per user.** Pages connect only to the relay, which carries live queries, notifications, events and presence. The relay registers each socket's conditions with this service over one connection and routes the matches.
- **Live queries and replication are the same thing.** A replica gets a token with `payload: raw` and no conditions, or a slice such as one tenant. Same matcher, same cursors, same resume.
- **Cursors instead of acknowledgements.** Every message carries a cursor; a consumer that reconnects sends the last one back. No acknowledgement on top of TCP, nothing lost.
- **Each destination has its own cursor.** Webhooks are batched and signed, and a failing receiver waits and retries without holding up the others or skipping anything. At the retry limit it pauses or dead-letters, chosen per destination.
- **Durable without a disk of its own.** The service confirms its place in the slot only after a segment is safely in object storage. Until then Postgres keeps the WAL, so a crash re-reads and loses nothing.
- **The archive is Iceberg.** Every change becomes a row in one Iceberg table per resource, readable by DuckDB, Spark, Trino and ClickHouse. With short retention it is cheap enough for every app, and the same files give record history, an audit log, replay and analytics.
- **Renames survive history.** Schema changes travel in the WAL in order with the rows, and columns keep stable field ids, so old versions read under today's names.
- **Erasure works on an archive.** Personal fields are encrypted per person when archived; deleting that person's key erases them in every copy.
- **Push uses the same stream.** A notification is a row naming its audience. The push sender expands the audience at delivery, skips people who are online, renders once per language, and retracts the push when the row is deleted. Without this service, the app calls the same sender directly.

## Protocols

- **Sockets:** Phoenix V2 frames, so stock Phoenix clients work. Pages use the relay's WebSocket, or SSE where proxies block WebSockets.
- **Pull:** WebSocket, SSE and HTTP long polling, each resuming from `?cursor=`.
- **Events:** CloudEvents in every transport. Webhooks get CloudEvents batches signed with Standard Webhooks.

## A live token

A list or record request opts in with the standard `Prefer` header (RFC 7240). Without that header, the app builds no token at all. The token arrives in a response header, so the body stays plain data and caches and ETags are undisturbed:

```http
GET /api/v1/messages?channel_id=xyz
Prefer: live

HTTP/1.1 200 OK
Preference-Applied: live
Protobase-Live: eyJhbGciOiJkaXIiLCJlbmMiOiJBMjU2R0NNIn0...
Vary: Prefer
```

The token is a JWE, encrypted with AES-256-GCM under a key that the app, the relay and protobase-changes share. The client can read none of it. Decrypted:

```json
{
  "iss": "https://shop.example", "sub": "u42", "sid": "s9", "exp": 1795480000,
  "payload": "thin",
  "cursor": "0/1A2B3C",
  "subs": [ { "resource": "messages",
              "where": { "and": [ { "eq": ["channel_id", "xyz"] },
                                  { "eq": ["tenant_id", "t1"] } ] } } ]
}
```

- `sub` and `sid` bind this token to one user and session. The relay checks them against the socket, along with expiry and revocation, so a copied token is useless.
- `payload` sets the shape of each change: `thin` for live queries, `raw` for the stored rows a replica needs.
- `cursor` marks the snapshot the list was read from, so no change slips between the read and the subscription. Each notice carries a fresh cursor to resume from.
- `where` combines the request's filter, `channel_id`, with the access rule, `tenant_id`. protobase-changes only ever sees plain conditions like these.
- `subs` lists each resource by table name, or as `schema.table` outside the public schema. A table whose schema or name holds a dot is never sent, since its resource could name another table too. There is no wildcard: a full replica lists every resource, so a table added later is never granted by accident.

### Conditions

A `where` is one condition on the changed row, an object of one operator:

| Condition | Meets the row when |
| --- | --- |
| `{"eq": [column, value]}`, `neq`, `lt`, `lte`, `gt`, `gte` | the column `=`, `<>`, `<`, `<=`, `>`, `>=` the value |
| `{"in": [column, [value, ...]]}` | the column equals one of the values |
| `{"between": [column, low, high]}` | `low <= column <= high` |
| `{"is_null": column}` | the column is null |
| `{"and": [...]}`, `{"or": [...]}`, `{"not": condition}` | as in SQL |

- A value is a string, a number or a boolean. A number compares numerically, so `12.5` meets `12.50`; a string compares with the column's text byte by byte, as under the C collation; a boolean compares with `t` or `f`.
- Null works as in a SQL `WHERE`: comparing with it is unknown, and only a condition that holds lets a row in. So `neq` never meets a null; add `{"is_null": column}` with `or` for that.
- A subscription that requires a column to equal a value, through `eq` or `in` joined by `and` at its top, is found by a hash lookup of the changed row's value. Only the others are tested against every change of their resource, so tokens should lead with such an equality, as the access rule's `tenant_id` gives.

Each change goes out once, with every subscription of the token it concerns and what it did there: `enter` when the row meets the conditions now and did not before, `update` when it meets them now, and `leave` when it met them and no longer does or was deleted. A truncate leaves every subscription to its table. A row's values show only where a subscription has the row in: an `enter` carries no old row and a `leave` no new row.

The old row decides `enter` and `leave`, and under the default replica identity the WAL holds only its key. Without it, an update the conditions let in comes as `update`, and an update or delete that may have left is not sent at all, since telling would show a row the subscription may not read. <gap>Under the default replica identity, a subscription on columns outside the key cannot learn of an update that left it or of a delete because looking up the old row is not implemented.</gap>

Some rules need a subquery, such as "channels I belong to". The app turns the subquery's result into constants when it issues the token. Could that go stale? Yes. When a membership changes, the app calls `live.refresh({ user })`, and the token's expiry caps anything missed.

## Self-hosting

**Development:** `protobase dev` sets it up for you. It downloads protobase-changes, starts it next to your app and the relay, and keeps the archive in `.protobase/changes/` with a short retention.

**Production:** run it next to your app and the relay:

```sh
protobase-changes --config changes.json
```

- **Download:** one binary or container image per GitHub release, like the relay.
- **State:** destination cursors go in a `changes` schema in your app's database, or in a SQLite file.
- **Updates:** `protobase-changes --config changes.json --migrate` applies the schema of each app database, its [revocations](#revocation) table, and of each store, then exits. Run it at each update, before the new version starts; it applies what is missing in one transaction for each app database and one for each store, also when they share a database. A failure leaves what that transaction would have changed as it was, while what an earlier transaction applied stays. Startup never migrates: an app whose database or store is behind serves or delivers nothing until it is migrated, and the other apps run.
- **Archive:** any S3-compatible bucket or a local directory.

### Configuration

The config lists every app database to follow; unknown fields stop startup.

```json
{
  "listen": ":8080",
  "apps": [
    {
      "name": "shop",
      "database_url": "postgres://changes@db.internal/shop",
      "slot": "protobase_changes",
      "publication": "protobase_changes",
      "tail_transactions": 10000,
      "issuer": "https://shop.example",
      "key_files": ["/run/secrets/shop_changes_key"],
      "store": { "kind": "postgres", "url": "postgres://changes@db.internal/shop" },
      "functions_url": "http://shop.internal/api/functions",
      "quotas": { "changes_per_second": 5000, "connections": 200 }
    }
  ]
}
```

- `listen`: the address the pull and destination endpoints serve on.

- `slot`: the logical replication slot, created with the `pgoutput` plugin on first start and reused after. A slot of that name with another plugin or on another database stops startup; it is never dropped. A slot Postgres invalidated is dropped and created again, as [Running without it](#running-without-it) says.
- `publication`: must exist before startup.
- Slot and publication names are lowercase letters, digits and underscores, up to 63 characters, not starting with a digit.
- `tail_transactions`: how many of the latest committed transactions stay in memory.
- `issuer`: the `iss` of the app's tokens, and the `source` of its events.
- `key_files`: files that each hold one 32-byte key the app shares, base64url without padding. The first one encrypts cursors; every one opens tokens and cursors, so a new key goes first and the old one stays listed until what it sealed has expired.
- `archive`: optional, the app's Iceberg archive below. Without it nothing confirms the slot, so Postgres keeps all WAL from the slot's start.
- `store`: where [destinations](#destinations) and their cursors are kept, `{"kind": "postgres", "url": ...}` for a `changes` schema in that database, or `{"kind": "sqlite", "path": ...}` for a file. Without it the app has no destinations.
- `functions_url`: where the app serves its API functions, which [function destinations](#functions) call by name.
- `quotas`: bounds on the app's share of a process that serves many apps, each unlimited when left out:
  - `changes_per_second`: the row changes its stream takes in per second, a second's worth at once. An app writing faster falls behind on its own and the other apps keep their pace. Postgres keeps its WAL meanwhile, so nothing is lost while the lag stays under `max_slot_wal_keep_size`; past it, the slot is invalidated, as [Running without it](#running-without-it) says.
  - `connections`: the consumers pulling its changes at once, over every transport; one more gets `429`.

### Many apps

One process follows every app its config lists, each with its own replication connection, slot, feed and destination workers, and nothing on one app's path is shared with another. An app whose database or store fails holds back only itself: it reads or delivers again after a wait of a second, doubling up to a minute, and back to a second once it ran a minute, while the other apps go on. Startup still stops on an invalid config or an unreadable key file. An app whose store is behind or cannot be reached at startup delivers nothing until it is migrated or reachable, while the other apps run; until its destinations have loaded, puts and resumes of them get `503`.

### Archive

Every change becomes one row of an Iceberg table per Postgres table, committed through the app's own Iceberg catalog:

```json
"archive": {
  "catalog": {
    "type": "rest",
    "url": "https://catalog.example/catalog",
    "warehouse": "shop",
    "credential": "client_id:client_secret",
    "properties": { "s3.endpoint": "https://s3.example", "s3.region": "eu-1" }
  },
  "namespace": "shop",
  "segment_bytes": 67108864,
  "segment_seconds": 900,
  "retention_hours": 168
}
```

- `catalog`: an Iceberg REST catalog at `url`, with its `warehouse`, and an OAuth2 client `credential` or a bearer `token` when it needs one. `properties` pass on to the catalog client and its file IO as they are, such as `s3.access-key-id` and `s3.secret-access-key` when the catalog vends no storage credentials. For development, `"type": "sql"` keeps the catalog in a SQLite file at `path` and new tables at `warehouse`, a `file://` folder or an `s3://` bucket path.
- `namespace`: the app's Iceberg namespace. It holds a namespace per Postgres schema, each with a table per Postgres table, such as `shop.public.messages`.
- `segment_bytes`, `segment_seconds`: changes collect in memory until about `segment_bytes` of values or `segment_seconds` have passed since the first, at most 900, then go out as one Parquet file per table.
- `retention_hours`: how long changes and table snapshots are kept, at most 2562047 (about 292 years).

Each row holds `commit_lsn`, `xid`, `commit_time`, `seq` (the change's position in its transaction), `operation`, `actor` (the transaction's last `protobase_actor` message before the change), `unchanged` (the columns whose large values an update left unchanged and the WAL did not repeat), and three structs with a field per column: `key`, `old` and `new`, each null when the WAL sent no such row. A struct joins the table once a change has columns for it, as Parquet stores no empty struct. Booleans, integers, floats, bytea, dates and timestamps keep their types; every other type, such as numeric, uuid, json or time, is stored in its Postgres text format. A time stays text because Postgres allows `24:00:00`, which an Iceberg time cannot hold. <gap>A timestamp after the year 294246 stops the archive because Iceberg counts microseconds from 1970 and storing such a timestamp in another form is not implemented.</gap>

Tables are partitioned by the hour of `commit_time`. Once an hour, every table of the app drops the hours that ended before its retention began, expires the snapshots older than the retention, and merges each finished hour's segment files.

**Schema changes** follow the WAL in order with the rows, and fields keep their Iceberg field ids:

- A new column adds a field. A dropped column keeps its field, null from then on.
- A widening from int to bigint or from real to double precision promotes the field.
- Any other type change moves the old field aside as `<column>__1` (or the next free number), where its values stay readable, and adds a new field. A field moved aside carries a doc that marks it retired, and no column writes to it again: when a new column takes its name, it moves aside once more.
- A rename keeps the field id when the app announces it in the same transaction as the `ALTER TABLE`, as the WAL alone cannot tell a rename from a drop and an add:

  ```sql
  SELECT pg_logical_emit_message(true, 'protobase_schema',
    '{"rename":{"schema":"public","table":"messages","from":"text","to":"body"}}');
  ```

  Without it, the new name becomes a new field. The renamed field's doc names the rename and its WAL position, and any other field of the new name, in any of the three structs, moves aside.

**Durability:** a segment commits table by table, each in one catalog commit together with the position it reached, and the slot is confirmed only after every table of the segment committed. A schema change splits a table's commit: the rows before it commit first, then each of its schema updates in a catalog commit of its own, then the rows after it, so the rows of every commit name the fields of the schema they commit with. After a crash the service reads the unconfirmed changes again, and each table skips what it already holds, so nothing is lost or archived twice. The archive must keep up with the tail: when the tail drops a transaction the archive has not read, the service stops, and the next start reads it again from the slot. A slot Postgres invalidated leaves the archive without the WAL it lost, and archiving goes on from the new slot. While nothing waits to be archived, the archive confirms the position the server's keepalives report, taken while no transaction was open, at most a segment's time late, so an idle app database does not hold the WAL other databases on the server write. <gap>Personal fields cannot be erased from the archive because per-person encryption is not implemented.</gap>

### Postgres requirements

- `wal_level = logical`, which takes a server restart.
- A role with `REPLICATION` that may connect to the app database.
- A publication of every change of all tables, created once by a superuser:

  ```sql
  CREATE PUBLICATION protobase_changes FOR ALL TABLES;
  ```

  Startup stops when it is missing or skips inserts, updates, deletes or truncates. Once it exists, Postgres refuses UPDATE and DELETE on a table without a primary key or another replica identity.
- A role that may create the `changes` schema in each app database for `--migrate`. The service's role needs `USAGE` on the schema and `SELECT` on `changes.revocations` and `changes.app_migrations`, and the app's role `USAGE`, `INSERT` and `DELETE` on `changes.revocations` and `SELECT` on its `exp` column, which its expired-row delete reads. Grant them once after the first `--migrate` when they are other roles.
- With a Postgres `store`, a role that may create the `changes` schema for `--migrate`, and read and write its tables after.

### Row changes

Each committed transaction arrives once, in commit order, with its id, commit LSN, end LSN, commit time and row changes. A change names its table, operation (insert, update, delete or truncate), key, and the old and new rows as the WAL states them:

- Values keep Postgres' text format, and a null stays distinct from a large value an update left unchanged, which the WAL does not repeat.
- The old row is only the key, and only on a delete or an update that changes the key, unless the table has `REPLICA IDENTITY FULL`; then it is the whole row.
- The key is the primary key or replica identity index. <gap>Tables with REPLICA IDENTITY FULL get no key because a primary key lookup is not implemented.</gap>

A new slot exports a snapshot of the database exactly where its stream starts; when taking it fails, the new slot is dropped so the next start takes it again. <gap>The initial state cannot reach anyone because no consumer of the exported snapshot is implemented.</gap>

The service never confirms its place in the slot by reading: Postgres keeps the WAL until the archive has committed it.

### Logical messages

The app writes transactional logical messages into the WAL, in the same transaction as its rows. Every message whose prefix starts with `protobase_` is attached to its transaction, in order, with its position among the row changes:

```sql
SELECT pg_logical_emit_message(true, 'protobase_actor', '{"sub":"u42","sid":"s9"}');
SELECT pg_logical_emit_message(true, 'protobase_schema', '{"rename":{"schema":"public","table":"messages","from":"text","to":"body"}}');
```

- `protobase_actor`: who made the changes that follow it.
- `protobase_schema`: a schema change, in order with the rows around it, such as the column rename the archive reads.

The content passes on unchanged as bytes; the payloads above are examples. Messages with other prefixes are skipped. A non-transactional `protobase_` message is skipped with a warning, since it arrives even when its transaction rolls back.

## Pulling changes

A replica or integration reads an app's changes from one address, in one of three transports, each with a bearer token:

```http
GET /apps/shop/changes?cursor=Ab3x...
Authorization: Bearer eyJhbGciOiJkaXIiLCJlbmMiOiJBMjU2R0NNIn0...
```

- **WebSocket:** an upgrade request, with subprotocol `cloudevents.json`: one event per text message.
- **Server-sent events:** `Accept: text/event-stream`. Each event's `id` is its cursor, so a reconnecting event source resumes through `Last-Event-ID` by itself.
- **Long polling:** any other request. It answers with a JSON array of events (`application/cloudevents-batch+json`) as soon as there is one, up to 500, or with a position event after 25 seconds.

The token is the live token above, with `payload: raw` for a replica. Without `?cursor=` the consumer starts at the token's `cursor`, or live from now. It gets the transactions held after that position, then live ones as they commit.

### Events

Every message is a CloudEvent in the structured JSON format, one per committed transaction, with the changes the token may read:

```json
{
  "specversion": "1.0", "id": "kR2vB8...", "source": "https://shop.example",
  "type": "protobase.changes.transaction", "time": "2026-11-24T10:00:00.123456Z",
  "datacontenttype": "application/json", "cursor": "Ab3x...",
  "data": { "changes": [
    { "resource": "messages", "operation": "update", "key": { "id": "42" },
      "new": { "id": "42", "body": "hey", "edited_at": null }, "actor": { "sub": "u42" },
      "subscriptions": [ { "id": 0, "effect": "update" } ] } ] }
}
```

- A `thin` payload holds the resource, operation and key; `raw` adds the old and new rows as the WAL states them, a truncate's `cascade` and `restart_identity`, and the actor of the last `protobase_actor` message before the change. A large value an update left unchanged is left out of `new`, never sent as null.
- `subscriptions` names each subscription the change concerns by its position in the token's `subs`, with its effect, as [Conditions](#conditions) says.
- The `id` stays the same on every replay of a transaction, so a consumer can drop a duplicate. It derives from the last key in `key_files`, so adding a new key first keeps it; retiring that last key changes every id.
- A transaction the token may read nothing of sends nothing. When nothing was sent for 25 seconds, a `protobase.changes.position` event without data carries the cursor past what was skipped, which keeps the connection alive and the consumer's cursor current.

### Cursors

Every event carries a `cursor`: the transaction's commit position and the database's epoch, its system identifier and timeline, encrypted with AES-256-GCM under a key derived from the app's key. A consumer keeps the cursor of the last event it handled and sends it back on its next connection; nothing is acknowledged. Cursors cannot be read or forged, and every cursor of one position resumes the same way.

The service holds the last `tail_transactions` transactions in memory, from where its stream started. A cursor it can no longer resume from gets `410`, and the consumer resyncs from a snapshot; a WebSocket or event stream that falls that far behind is closed, and its reconnect gets the `410`.

- <gap>A cursor older than the in-memory tail cannot resume because replay from the archive is not implemented.</gap>
- <gap>Cursors cannot survive a failover or a restore because a mapping between timelines is not implemented.</gap>

### Revocation

The app revokes a session with a row in `changes.revocations`, in the transaction that ends it, with `exp` the Unix time the session's last token expires:

```sql
DELETE FROM changes.revocations WHERE exp <= extract(epoch FROM now());
INSERT INTO changes.revocations (sid, exp) VALUES ('s9', 1795480000);
```

A stream of that session stops before any change committed after the revocation, and new requests get `401`. A stream also ends when its token expires, and the consumer reconnects with a fresh token and its cursor. The service remembers a revoked session until its `exp`. Only an insert revokes: updating or deleting a row lifts nothing. The app deletes the rows whose `exp` has passed, such as in the same transaction as above, so the table holds only revocations in force. Before a stream serves, it reads every revocation still in force from the table, inside a new slot's exported snapshot or, for a slot it reuses, as the table stands then, and the WAL brings the ones after. So a restart, or a slot that lost its WAL, still knows every revocation, and the slot never keeps WAL for one. An app database without the table answers `503` until `--migrate` has run.

### Answers

- `400`: an invalid cursor, or invalid subscription conditions.
- `401`: no valid token for the app, an expired one, or a revoked session. Every invalid token gets the same answer.
- `404`: no app of that name.
- `410`: the cursor can no longer resume.
- `429`: the app has as many open connections as its `connections` quota allows.
- `503`: the app's stream has not yet read its revocations and all the WAL there was when it connected, so revocations may still be unknown.

## Destinations

Webhooks and the app's own functions get changes in signed batches. Each destination reads the same stream from a cursor of its own, so a change is still one write, and a failing receiver holds back only itself and skips nothing.

### Defining them

The app holds its destinations, such as in its config, and puts all of them at once whenever they change, such as at each deploy:

```http
PUT /apps/shop/destinations
Content-Type: application/jose

eyJhbGciOiJkaXIiLCJlbmMiOiJBMjU2R0NNIn0...
```

The body is a token encrypted like a live token, so its secrets stay sealed in transit and in the store, which keeps the token as it came. Decrypted:

```json
{
  "iss": "https://shop.example", "iat": 1795480000,
  "destinations": [
    { "name": "orders-hook", "url": "https://erp.example/hooks/orders",
      "secret": "whsec_...",
      "payload": "raw", "batch_size": 100,
      "subs": [ { "resource": "orders", "where": { "eq": ["tenant_id", "t1"] } } ],
      "retry": { "tries": 10, "at_limit": "pause" } },
    { "name": "paid", "function": "route-paid", "url": "https://mail.example/hooks",
      "secret": "whsec_...", "payload": "thin",
      "events": [ { "type": "orders.paid", "resource": "orders", "operations": ["insert", "update"],
                    "old": { "neq": ["status", "paid"] }, "new": { "eq": ["status", "paid"] } } ],
      "retry": { "at_limit": "dead_letter" } }
  ]
}
```

- `iat` orders the sets: one issued before the kept set is refused with `409`, and putting the kept token again changes nothing. The token has no `exp`, since it stays until a later set replaces it.
- `name`: letters, digits, `_` and `-`, starting with a letter. A destination keeps its cursor while its name stays; one the set leaves out loses it, and a new one starts at the newest change.
- `url` gets each batch, and `function` names an API function that gets it first. A destination has one or both.
- `secret` signs every request, as Standard Webhooks describes: `whsec_` and base64.
- `payload`: `thin` or `raw`, as for a live token.
- `subs`: subscriptions as a live token lists them; each transaction with changes they concern becomes one `protobase.changes.transaction` event.
- `events`: [named events](#named-events).
- `batch_size`: at most this many events per batch, 1 to 500, 100 by default. A batch holds whole transactions, so one large transaction can pass it.
- `retry.tries`: how often a batch is sent before `at_limit` applies, 1 to 10, 10 by default. `retry.at_limit` is `pause` or `dead_letter`.

Unknown fields refuse the set with `400`; a token that does not open, or a live token, gets `401`; an app without a `store` gets `404`.

### Named events

A named event, such as `orders.paid`, is raised by one change of `resource` that is one of `operations` (all of `insert`, `update` and `delete` when left out) and meets two [conditions](#conditions): `old` on the row before the change, where there is one, and `new` on the row after it, where there is one. Each is its own CloudEvent of that type, with the one change as its data. `orders.paid` above is a new order that is paid, or an order whose status turned paid.

Under the default replica identity the WAL states only the key of the old row, so an `old` condition on any other column fails and the event is not raised. Give such a table `REPLICA IDENTITY FULL`. <gap>A named event's old condition cannot read a column outside the key under the default replica identity because looking up the old row is not implemented.</gap>

### What a receiver gets

Each batch is a `POST` of a JSON array of CloudEvents, `Content-Type: application/cloudevents-batch+json`, in commit order, as [Events](#events) shows them. It carries the Standard Webhooks headers:

- `webhook-id`: the batch's id, the same on every try of it, so a receiver can drop a repeat. Each event keeps its own stable `id` too.
- `webhook-timestamp`: the try's time in seconds.
- `webhook-signature`: `v1,` and the base64 HMAC-SHA256 of `id.timestamp.body` under the secret.

Any `2xx` delivers the batch, and the destination's cursor moves past it with one write to the store. A transaction the destination takes nothing from moves its cursor in memory, which is written only once it has passed half of `tail_transactions`, so a quiet destination writes seldom and a restart still finds its cursor among the transactions held.

### Functions

A destination with a `function` posts each batch, signed the same way, to `functions_url` and the function's name, such as `/api/functions/route-paid`. A public function checks the signature with `verifyStandardWebhook`. Its answer decides what happens:

- An empty body, `null` or `[]`: the batch is dropped, and the cursor moves past it.
- A JSON array of events: they go to the destination's `url` as the batch, under the same `webhook-id`. A destination without a `url` counts this as a failed try.

Anything else, and a function that fails, is a failed try, and the next try calls the function again.

### Retries

A failed try is any answer outside `2xx`, or none within 30 seconds. The same batch is tried again after 5 seconds, 5 minutes, 30 minutes, 2, 5, 10, 14, 20 and 24 hours, or after the receiver's `Retry-After` when that is longer, up to a day. Retries wait in the store, so a restart picks them up with the same batch and id. A failing destination never holds back another.

After its last try, a destination with `at_limit: pause` stops at the batch. One with `dead_letter` keeps the batch in the store's `dead_letters` table and goes on after it. The app resumes a paused destination with a token that names it:

```http
POST /apps/shop/destinations/orders-hook/resume
Authorization: Bearer eyJhbGciOiJkaXIiLCJlbmMiOiJBMjU2R0NNIn0...
```

The token holds `iss`, `exp` and `"resume": "orders-hook"`. The destination tries the same batch again, with every try back. Resuming a destination that is waiting for a retry tries it now.

A destination whose cursor falls behind what the service holds in memory is `gone`: resuming it starts it again at the newest change. <gap>A destination that falls behind the in-memory tail cannot catch up because replay from the archive is not implemented.</gap>

### The store

The store keeps the set token in `definitions`, each destination's cursor, status (`active`, `paused` or `gone`) and failed batch in `states`, and dead letters in `dead_letters`. In the app's own database, the app reads them with SQL. <gap>The app cannot read a destination's status or dead letters through the service because a status endpoint is not implemented.</gap> <gap>Dead letters cannot be sent again because a redrive is not implemented.</gap>

The store's writes are changes like any other, so they reach the stream too; a destination that does not subscribe to them passes each without a write of its own, so they never feed themselves.

## Running without it

A small app that needs no webhooks, replication or history can skip protobase-changes.

- **Offline for a while:** nothing is lost. The slot holds the WAL up to `max_slot_wal_keep_size`. Webhooks, subscriptions and push catch up when the service returns, and live pages poll meanwhile.
- **Not configured:** reads, writes and the API work as normal, and live pages poll. Push still works: the app calls the push sender directly once the notification row commits.
- **Unavailable without it:** webhooks, event subscriptions, replication and history. `protobase doctor` names what to run. An app that declares them gets a startup warning, not a crash.

An outage that outlasts that WAL limit invalidates the slot. The service then drops it and creates it again with a new exported snapshot, while the other apps go on. The changes in the WAL the slot lost are gone, but its revocations are not: the new stream reads them from the [revocations](#revocation) table before it serves.

Once a slot is created again, its stream starts there: every older cursor gets `410` and resyncs, and every destination becomes `gone`. A stream open at the time ends at once, and its reconnect gets the `410`. The new slot starts past every position the old one gave, so its start alone tells old cursors from new ones.

The service confirms its place only once the archive holds the WAL, so until then a finite `max_slot_wal_keep_size` also invalidates a slot that keeps up, once that much WAL of the whole server has passed the slot's start; with the default `-1`, Postgres keeps the WAL without bound. <gap>A slot that keeps up cannot stay valid under a finite max_slot_wal_keep_size because confirming the slot once the archive holds the WAL is not implemented.</gap>

## License

Apache-2.0. See [LICENSE](LICENSE).
