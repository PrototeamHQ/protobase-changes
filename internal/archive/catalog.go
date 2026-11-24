package archive

import (
	"context"
	"database/sql"
	"fmt"
	"maps"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	sqlcatalog "github.com/apache/iceberg-go/catalog/sql"
	// Registers the s3:// file IO; file:// is built in.
	_ "github.com/apache/iceberg-go/io/gocloud/s3"
	"github.com/uptrace/bun/driver/sqliteshim"

	"github.com/PrototeamHQ/protobase-changes/internal/config"
)

// Open connects to an app's catalog. Both kinds sit behind iceberg-go's
// catalog interface, so the archive works the same on either.
func Open(ctx context.Context, app string, c config.Catalog) (catalog.Catalog, error) {
	props := iceberg.Properties(maps.Clone(c.Properties))
	switch c.Type {
	case config.RESTCatalog:
		opts := []rest.Option{rest.WithAdditionalProps(props)}
		if c.Warehouse != "" {
			opts = append(opts, rest.WithWarehouseLocation(c.Warehouse))
		}
		if c.Credential != "" {
			opts = append(opts, rest.WithCredential(c.Credential))
		}
		if c.Token != "" {
			opts = append(opts, rest.WithOAuthToken(c.Token))
		}
		cat, err := rest.NewCatalog(ctx, app, c.URL, opts...)
		if err != nil {
			return nil, fmt.Errorf("open REST catalog: %w", err)
		}
		return cat, nil
	case config.SQLCatalog:
		db, err := sql.Open(sqliteshim.ShimName, c.Path)
		if err != nil {
			return nil, fmt.Errorf("open SQL catalog: %w", err)
		}
		if props == nil {
			props = iceberg.Properties{}
		}
		props["warehouse"] = c.Warehouse
		cat, err := sqlcatalog.NewCatalog(app, db, sqlcatalog.SQLite, props)
		if err != nil {
			db.Close()
			return nil, fmt.Errorf("open SQL catalog: %w", err)
		}
		return cat, nil
	}
	// config.Parse refuses any other type.
	panic("unknown catalog type " + c.Type)
}
