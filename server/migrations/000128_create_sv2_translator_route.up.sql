-- Each distinct SV2 destination gets a stable local SV1 listener. Routes are
-- intentionally retained when a saved pool is edited or deleted: miners that
-- were already assigned the old route must keep mining until they are
-- explicitly reassigned.
CREATE SEQUENCE sv2_translator_port_seq
    AS INTEGER
    MINVALUE 34255
    MAXVALUE 65535
    START WITH 34255
    NO CYCLE;

CREATE TABLE sv2_translator_route (
    id            BIGSERIAL PRIMARY KEY,
    org_id        BIGINT NOT NULL,
    upstream_url  VARCHAR(256) NOT NULL,
    username      VARCHAR(255) NOT NULL,
    listen_port   INTEGER NOT NULL DEFAULT nextval('sv2_translator_port_seq'),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT uk_sv2_translator_route_destination
        UNIQUE (org_id, upstream_url, username),
    CONSTRAINT uk_sv2_translator_route_listen_port
        UNIQUE (listen_port),
    CONSTRAINT ck_sv2_translator_route_url
        CHECK (upstream_url LIKE 'stratum2+tcp://%'),
    CONSTRAINT ck_sv2_translator_route_port
        CHECK (listen_port BETWEEN 34255 AND 65535)
);

ALTER SEQUENCE sv2_translator_port_seq
    OWNED BY sv2_translator_route.listen_port;
