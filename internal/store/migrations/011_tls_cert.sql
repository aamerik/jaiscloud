CREATE TABLE IF NOT EXISTS jc_tls_cert (
    id         TEXT        PRIMARY KEY DEFAULT 'singleton',
    cert_pem   BYTEA       NOT NULL,
    key_pem    BYTEA       NOT NULL,
    not_after  TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
