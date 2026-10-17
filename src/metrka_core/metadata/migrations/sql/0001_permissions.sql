-- The application-schema grants and append-only restrictions are part of the
-- canonical schema dump in 0001_initial.sql. Alembic creates its version table
-- in public, so its narrowly scoped runtime grants remain explicit here.

GRANT USAGE ON SCHEMA public TO metrka_migrator, metrka_etl;

GRANT SELECT ON TABLE public.alembic_version
TO metrka_migrator, metrka_etl;
