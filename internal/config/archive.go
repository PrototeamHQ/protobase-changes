package config

import (
	"fmt"
	"math"
	"net/url"
	"time"
)

// MaxSegmentSeconds bounds how long a change waits in memory before it is archived.
const MaxSegmentSeconds = 900

// MaxRetentionHours is the longest retention a time.Duration holds: a longer
// one would wrap around to a negative duration and expire everything.
const MaxRetentionHours = math.MaxInt64 / int64(time.Hour)

// Archive stores every change of an app in one Iceberg table per Postgres table.
type Archive struct {
	Catalog Catalog `json:"catalog"`
	// Namespace is the Iceberg namespace of the app. It holds one child
	// namespace per Postgres schema, each with one table per Postgres table.
	Namespace string `json:"namespace"`
	// SegmentBytes and SegmentSeconds roll a segment over to the archive once
	// it holds about that many bytes of values or is that old.
	SegmentBytes   int64 `json:"segment_bytes"`
	SegmentSeconds int   `json:"segment_seconds"`
	// RetentionHours is how long changes and table snapshots are kept.
	RetentionHours int `json:"retention_hours"`
}

// Catalog types.
const (
	// RESTCatalog is an Iceberg REST catalog, the production path.
	RESTCatalog = "rest"
	// SQLCatalog is a SQLite file catalog, for development only.
	SQLCatalog = "sql"
)

// Catalog is the Iceberg catalog the archive commits through.
type Catalog struct {
	Type string `json:"type"`
	// URL is the REST catalog's base URL.
	URL string `json:"url"`
	// Warehouse is the REST catalog's warehouse, or, for a SQL catalog, the
	// location new tables go in: a file:// folder or an s3:// bucket path.
	Warehouse string `json:"warehouse"`
	// Path is the SQLite file of a SQL catalog.
	Path string `json:"path"`
	// Credential is an OAuth2 client credential, "client_id:client_secret",
	// and Token a bearer token, for a REST catalog that needs either.
	Credential string `json:"credential"`
	Token      string `json:"token"`
	// Properties pass on to the catalog and its file IO as they are, such as
	// s3.endpoint, s3.access-key-id and s3.secret-access-key when the catalog
	// vends no storage credentials.
	Properties map[string]string `json:"properties"`
}

func (a Archive) validate(app string) error {
	if !identifier.MatchString(a.Namespace) {
		return fmt.Errorf("config: app %q archive namespace must be 1 to 63 lowercase letters, digits or underscores, not starting with a digit", app)
	}
	if a.SegmentBytes < 1 {
		return fmt.Errorf("config: app %q archive segment_bytes must be at least 1", app)
	}
	if a.SegmentSeconds < 1 || a.SegmentSeconds > MaxSegmentSeconds {
		return fmt.Errorf("config: app %q archive segment_seconds must be 1 to %d", app, MaxSegmentSeconds)
	}
	if a.RetentionHours < 1 || int64(a.RetentionHours) > MaxRetentionHours {
		return fmt.Errorf("config: app %q archive retention_hours must be 1 to %d", app, MaxRetentionHours)
	}
	return a.Catalog.validate(app)
}

func (c Catalog) validate(app string) error {
	switch c.Type {
	case RESTCatalog:
		u, err := url.Parse(c.URL)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return fmt.Errorf("config: app %q archive catalog url must be an http or https URL", app)
		}
		if c.Path != "" {
			return fmt.Errorf("config: app %q archive catalog path is only for a sql catalog", app)
		}
		if c.Credential != "" && c.Token != "" {
			return fmt.Errorf("config: app %q archive catalog takes a credential or a token, not both", app)
		}
	case SQLCatalog:
		if c.Path == "" || c.Warehouse == "" {
			return fmt.Errorf("config: app %q archive sql catalog needs a path and a warehouse", app)
		}
		if c.URL != "" || c.Credential != "" || c.Token != "" {
			return fmt.Errorf("config: app %q archive sql catalog takes no url, credential or token", app)
		}
	default:
		return fmt.Errorf("config: app %q archive catalog type must be %q or %q", app, RESTCatalog, SQLCatalog)
	}
	return nil
}
