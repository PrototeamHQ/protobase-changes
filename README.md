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
- `subs` lists each resource by table name, or as `schema.table` outside the public schema. There is no wildcard: a full replica lists every resource, so a table added later is never granted by accident. <gap>A subscription with `where` is refused with 400 because the condition matcher is not implemented.</gap>

Some rules need a subquery, such as "channels I belong to". The app turns the subquery's result into constants when it issues the token. Could that go stale? Yes. When a membership changes, the app calls `live.refresh({ user })`, and the token's expiry caps anything missed.

## Self-hosting

**Development:** `protobase dev` sets it up for you. It downloads protobase-changes, starts it next to your app and the relay, and keeps the archive in `.protobase/changes/` with a short retention.

**Production:** run it next to your app and the relay:

```sh
protobase-changes --config changes.json
```

- **Download:** one binary or container image per GitHub release, like the relay.
- **State:** destination cursors go in a `changes` schema in your app's database.
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
      "key_files": ["/run/secrets/shop_changes_key"]
    }
  ]
}
```

- `listen`: the address the pull endpoints serve on.

- `slot`: the logical replication slot, created with the `pgoutput` plugin on first start and reused after. A slot of that name with another plugin or on another database stops startup; it is never dropped.
- `publication`: must exist before startup.
- Slot and publication names are lowercase letters, digits and underscores, up to 63 characters, not starting with a digit.
- `tail_transactions`: how many of the latest committed transactions stay in memory.
- `issuer`: the `iss` of the app's tokens, and the `source` of its events.
- `key_files`: files that each hold one 32-byte key the app shares, base64url without padding. The first one encrypts cursors; every one opens tokens and cursors, so a new key goes first and the old one stays listed until what it sealed has expired.

### Postgres requirements

- `wal_level = logical`, which takes a server restart.
- A role with `REPLICATION` that may connect to the app database.
- A publication of every change of all tables, created once by a superuser:

  ```sql
  CREATE PUBLICATION protobase_changes FOR ALL TABLES;
  ```

  Startup stops when it is missing or skips inserts, updates, deletes or truncates. Once it exists, Postgres refuses UPDATE and DELETE on a table without a primary key or another replica identity.

### Row changes

Each committed transaction arrives once, in commit order, with its id, commit LSN, end LSN, commit time and row changes. A change names its table, operation (insert, update, delete or truncate), key, and the old and new rows as the WAL states them:

- Values keep Postgres' text format, and a null stays distinct from a large value an update left unchanged, which the WAL does not repeat.
- The old row is only the key, and only on a delete or an update that changes the key, unless the table has `REPLICA IDENTITY FULL`; then it is the whole row.
- The key is the primary key or replica identity index. <gap>Tables with REPLICA IDENTITY FULL get no key because a primary key lookup is not implemented.</gap>

A new slot exports a snapshot of the database exactly where its stream starts; when taking it fails, the new slot is dropped so the next start takes it again. <gap>The initial state cannot reach anyone because no consumer of the exported snapshot is implemented.</gap>

The service never confirms its place in the slot by reading: Postgres keeps the WAL until the archive has stored it.

### Logical messages

The app writes transactional logical messages into the WAL, in the same transaction as its rows. Every message whose prefix starts with `protobase_` is attached to its transaction, in order, with its position among the row changes:

```sql
SELECT pg_logical_emit_message(true, 'protobase_actor', '{"sub":"u42","sid":"s9"}');
SELECT pg_logical_emit_message(true, 'protobase_schema', '{"rename":{"table":"messages","from":"text","to":"body"}}');
```

- `protobase_actor`: who made the changes that follow it.
- `protobase_schema`: a schema change, in order with the rows around it.
- `protobase_revoke`: ends a session's access, as `{"sid":"s9","exp":1795480000}` with `exp` when its last token expires.

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
      "new": { "id": "42", "body": "hey", "edited_at": null }, "actor": { "sub": "u42" } } ] }
}
```

- A `thin` payload holds the resource, operation and key; `raw` adds the old and new rows as the WAL states them, a truncate's `cascade` and `restart_identity`, and the actor of the last `protobase_actor` message before the change. A large value an update left unchanged is left out of `new`, never sent as null.
- The `id` stays the same on every replay of a transaction, so a consumer can drop a duplicate.
- A transaction the token may read nothing of sends nothing. When nothing was sent for 25 seconds, a `protobase.changes.position` event without data carries the cursor past what was skipped, which keeps the connection alive and the consumer's cursor current.

### Cursors

Every event carries a `cursor`: the transaction's commit position and the database's epoch, its system identifier and timeline, encrypted with AES-256-GCM under a key derived from the app's key. A consumer keeps the cursor of the last event it handled and sends it back on its next connection; nothing is acknowledged. Cursors cannot be read or forged, and every cursor of one position resumes the same way.

The service holds the last `tail_transactions` transactions in memory, from where its stream started. A cursor it can no longer resume from gets `410`, and the consumer resyncs from a snapshot; a WebSocket or event stream that falls that far behind is closed, and its reconnect gets the `410`.

- <gap>A cursor older than the in-memory tail cannot resume because replay from the archive is not implemented.</gap>
- <gap>Cursors cannot survive a failover or a restore because a mapping between timelines is not implemented.</gap>

### Revocation

The app revokes a session with a `protobase_revoke` message in the transaction that ends it. A stream of that session stops before any change committed after the revocation, and new requests get `401`. A stream also ends when its token expires, and the consumer reconnects with a fresh token and its cursor. The service remembers a revoked session until the `exp` the message gives. <gap>Revocations cannot outlive a restart once the archive confirms the slot because rebuilding them from the archive is not implemented.</gap>

### Answers

- `400`: an invalid cursor, or a token with conditions.
- `401`: no valid token for the app, an expired one, or a revoked session. Every invalid token gets the same answer.
- `404`: no app of that name.
- `410`: the cursor can no longer resume.
- `503`: the app's stream has not started yet.

## Running without it

A small app that needs no webhooks, replication or history can skip protobase-changes.

- **Offline for a while:** nothing is lost. The slot holds the WAL up to `max_slot_wal_keep_size`. Webhooks, subscriptions and push catch up when the service returns, and live pages poll meanwhile.
- **Not configured:** reads, writes and the API work as normal, and live pages poll. Push still works: the app calls the push sender directly once the notification row commits.
- **Unavailable without it:** webhooks, event subscriptions, replication and history. `protobase doctor` names what to run. An app that declares them gets a startup warning, not a crash.

An outage that outlasts that WAL limit invalidates the slot. The service rebuilds it, and old cursors get `410` and resync.

## License

Apache-2.0. See [LICENSE](LICENSE).
