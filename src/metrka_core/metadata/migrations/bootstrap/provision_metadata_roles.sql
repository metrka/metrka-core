\set ON_ERROR_STOP on

DO $roles$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_roles WHERE rolname = 'metrka_owner'
    ) THEN
        CREATE ROLE metrka_owner NOLOGIN;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_roles WHERE rolname = 'metrka_migrator'
    ) THEN
        CREATE ROLE metrka_migrator LOGIN;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_roles WHERE rolname = 'metrka_etl'
    ) THEN
        CREATE ROLE metrka_etl LOGIN;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_roles WHERE rolname = 'metrka_web'
    ) THEN
        CREATE ROLE metrka_web LOGIN;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_roles WHERE rolname = 'metrka_operator'
    ) THEN
        CREATE ROLE metrka_operator LOGIN;
    END IF;
END
$roles$;

ALTER ROLE metrka_owner
    NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;

ALTER ROLE metrka_migrator
    LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOINHERIT;

ALTER ROLE metrka_etl
    LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOINHERIT;

ALTER ROLE metrka_web
    LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOINHERIT;

ALTER ROLE metrka_operator
    LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOINHERIT;

GRANT metrka_owner TO metrka_migrator;

DO $operator_membership$
BEGIN
    IF pg_has_role('metrka_operator', 'metrka_owner', 'MEMBER') THEN
        EXECUTE 'REVOKE metrka_owner FROM metrka_operator';
    END IF;
END
$operator_membership$;

SELECT current_database() AS metrka_database \gset

ALTER DATABASE :"metrka_database" OWNER TO metrka_owner;
REVOKE CONNECT, TEMPORARY ON DATABASE :"metrka_database" FROM PUBLIC;
GRANT CONNECT ON DATABASE :"metrka_database"
TO metrka_migrator, metrka_etl, metrka_web, metrka_operator;

REVOKE CREATE ON SCHEMA public FROM PUBLIC;
