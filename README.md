# student360-dwh-relay

The data-warehouse feed, packaged as a **Cloud Run job**: drains the services' transactional
outbox tables (`support.outbox_event`, `network.outbox_event`) into the Pub/Sub topic
`student360-events`, whose BigQuery subscription lands every envelope in
`student360_dwh.outbox_events`. Cloud Scheduler runs it every 5 minutes; `gcloud run jobs
execute s360-relay` runs it by hand.

## The reliability contract

- Rows are claimed with `FOR UPDATE SKIP LOCKED`: overlapping runs skip each other's claims
  instead of blocking or double-publishing.
- A row is marked `published_at` **in the same transaction, only after the broker acked** the
  publish. A crash between publish and commit re-delivers next run: **at-least-once** by design.
- Deduplicate downstream by the envelope's own id:
  ```sql
  SELECT DISTINCT JSON_VALUE(data, '$.eventId') …
  ```
- No ordering keys: the BigQuery subscription writes unordered regardless, and the envelope
  carries `occurredAt` for query-time ordering.

The envelope was written by `student360-common`'s outbox publisher as the exact message a
subscriber should receive — this job publishes it verbatim, adding only routing attributes
(`eventType`, `aggregateType`, `aggregateId`, `sourceSchema`).

## Database access

Runs as the `dwh_relay` role, which holds only `SELECT, UPDATE` on those two schemas' tables
(granted by `terraform-core/scripts/sql/db-init.sql` via default privileges — Flyway creates the
outbox tables after the grant script runs).

## Verify

`mvn verify` — the integration test runs the whole contract against a real Postgres
(Testcontainers) and the real Pub/Sub wire protocol (the emulator container): batch paging,
marking, idempotent re-run, and the schema-identifier guard.
