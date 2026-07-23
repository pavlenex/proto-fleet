package sv2translator

import (
	"context"
	"database/sql"

	"github.com/block/proto-fleet/server/generated/sqlc"
	"github.com/block/proto-fleet/server/internal/infrastructure/db"
)

type Route struct {
	OrganizationID int64
	UpstreamURL    string
	Username       string
	ListenPort     int32
}

type RouteStore interface {
	GetOrCreate(ctx context.Context, organizationID int64, upstreamURL, username string) (Route, error)
	GetByPort(ctx context.Context, organizationID int64, listenPort int32) (Route, error)
}

type SQLRouteStore struct {
	conn *sql.DB
}

func NewSQLRouteStore(conn *sql.DB) *SQLRouteStore {
	return &SQLRouteStore{conn: conn}
}

func (s *SQLRouteStore) GetOrCreate(
	ctx context.Context,
	organizationID int64,
	upstreamURL string,
	username string,
) (Route, error) {
	row, err := db.WithTransaction(ctx, s.conn, func(q *sqlc.Queries) (sqlc.Sv2TranslatorRoute, error) {
		return q.GetOrCreateSV2TranslatorRoute(ctx, sqlc.GetOrCreateSV2TranslatorRouteParams{
			OrgID:       organizationID,
			UpstreamUrl: upstreamURL,
			Username:    username,
		})
	})
	if err != nil {
		return Route{}, err
	}
	return routeFromSQL(row), nil
}

func (s *SQLRouteStore) GetByPort(
	ctx context.Context,
	organizationID int64,
	listenPort int32,
) (Route, error) {
	row, err := db.WithTransaction(ctx, s.conn, func(q *sqlc.Queries) (sqlc.Sv2TranslatorRoute, error) {
		return q.GetSV2TranslatorRouteByPort(ctx, sqlc.GetSV2TranslatorRouteByPortParams{
			OrgID:      organizationID,
			ListenPort: listenPort,
		})
	})
	if err != nil {
		return Route{}, err
	}
	return routeFromSQL(row), nil
}

func routeFromSQL(row sqlc.Sv2TranslatorRoute) Route {
	return Route{
		OrganizationID: row.OrgID,
		UpstreamURL:    row.UpstreamUrl,
		Username:       row.Username,
		ListenPort:     row.ListenPort,
	}
}
