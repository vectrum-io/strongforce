-- Add new schema named "strongforce"
CREATE SCHEMA "strongforce";
-- Create "event_outbox" table
CREATE TABLE "strongforce"."event_outbox" ("id" character(36) NOT NULL, "topic" character varying(255) NOT NULL, "payload" bytea NOT NULL, "created_at" timestamp NULL DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY ("id"));
-- Create "test" table
CREATE TABLE "strongforce"."test" ("id" serial NOT NULL, "value" character varying(255) NOT NULL, PRIMARY KEY ("id"));
