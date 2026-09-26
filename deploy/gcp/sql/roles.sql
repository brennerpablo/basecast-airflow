-- Login roles and grants, run as `postgres` on the `postgres` database. Idempotent.
-- Passwords come in as psql variables (:airflow_pw, :writer_pw, :reader_pw) from Secret Manager.
-- Roles are created here, not with `gcloud sql users create`, because API-created users join
-- cloudsqlsuperuser and could write everywhere.

SELECT format('CREATE ROLE %I LOGIN', r)
FROM unnest(ARRAY['airflow', 'basecast_writer', 'basecast_reader']) AS r
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = r)
\gexec

ALTER ROLE airflow PASSWORD :'airflow_pw';
ALTER ROLE basecast_writer PASSWORD :'writer_pw';
ALTER ROLE basecast_reader PASSWORD :'reader_pw';

REVOKE CONNECT ON DATABASE airflow, basecast FROM PUBLIC;
GRANT CONNECT, TEMPORARY ON DATABASE airflow TO airflow;
GRANT CONNECT, TEMPORARY ON DATABASE basecast TO basecast_writer;
GRANT CONNECT ON DATABASE basecast TO basecast_reader;

\connect airflow
GRANT ALL ON SCHEMA public TO airflow;

\connect basecast
CREATE EXTENSION IF NOT EXISTS postgis;
GRANT USAGE, CREATE ON SCHEMA public TO basecast_writer;
GRANT USAGE ON SCHEMA public TO basecast_reader;

-- The `ops` schema (the shared log, ops.log) belongs to the app: basecast-app `npm run db:push` creates it
-- and basecast-app `prisma/ops-grants.sql` gives basecast_writer INSERT there.
