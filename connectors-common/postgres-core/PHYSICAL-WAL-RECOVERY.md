# Physical WAL recovery

## Recovery and standby preference

Physical CDC starts normally with the configured standby preference. A connector-owned
recovery state is shared by replacement miners; timeline changes and stream stalls
enter primary-first recovery. Retries of an active recovery retain this policy.
The periodic standby-preference checker does not change the
connection while recovery is active. An intentional standby handoff after recovery
does not itself start another primary-first cycle. On re-entry, the connector
revalidates standby health, so an idle stream can complete the handoff without new
writes. A concurrent real recovery takes precedence over a planned handoff.

If no primary is visible during an active recovery's connector retry, the connector returns a
retryable failure without advancing the offset. The task framework controls retries
and the eventual task status. During an existing miner's recovery, node probes retry
three times, one second apart, before using other configured sources. Source ordering
is current-timeline primary, current-timeline standbys ordered by readable LSN, then
remaining nodes. A newly observed higher timeline requests a fresh recovery attempt.

An ancestor-stream failure on one node does not establish that all WAL is gone.
The miner tries remaining configured nodes without skipping the requested range.
After ancestor catch-up it selects current-timeline sources again, preferring the primary.

## Completion and missing WAL

Opening a stream is not recovery completion. Standby preference resumes only after
the consumer has accepted an offset beyond the recovery stream's emit boundary on
the current timeline, ancestor catch-up is no longer pending, the active source is
verified as a current-timeline primary or standby, and no newer timeline is visible. Generation checks
prevent an older stream from completing a newer recovery. Idle streams can therefore
remain primary-pinned until they make confirmed progress.

Both ancestor catch-up failure and an unreadable saved offset refuse automatic
skip-forward by default. The existing `walUnsafeTimelineResume` operator override
permits unsafe skipping with a warning. It invalidates the old stream generation and
latches the new generation as unsafe: neither can report recovery completion or
automatically restore standby preference. Only a subsequent explicit recovery attempt
can clear that latch, and it must satisfy the same consumer-confirmed progress checks.
Completion describes that generation only; it does not restore previously skipped WAL.
No slot-synchronization plugin is required
by this selection logic, but the requested historical WAL must actually be readable.

When ancestor catch-up reports `requested WAL segment ... has already been
removed` or later becomes `ancestor catch-up stalled`, the miner first attempts
to replay the saved-offset-to-fork range from an independent WAL archive. The
archive must contain every required segment for the relevant timeline(s).
Archive replay uses the same page decoder, TDE WAL decryptor, transaction commit
gate, and consumer offset path as online streaming.

Configure either `walArchiveDir`, a directory visible to the CDC runtime, or
`walArchiveRestoreCommand`. The latter is a PostgreSQL-style restore hook:
`%f` is replaced by the WAL/history filename, `%p` by the destination path,
and `%t` by the timeline number. The equivalent environment variables are
`TAPDATA_WAL_ARCHIVE_DIR` and `TAPDATA_WAL_ARCHIVE_RESTORE_COMMAND`.
The restore hook runs only after a local lookup misses. If any required segment
is absent or replay does not reach the fork point, the task remains fatal by
default; file existence alone is never treated as successful consumption.
`walUnsafeTimelineResume` still controls the explicit skip-and-continue override.

Consumer acceptance here is **not proof of durable engine checkpoint persistence**.
This change does not fix MongoDB task-offset persistence and does not reconstruct
missing WAL or transactions absent from the promoted database.

Missing-WAL failures can be retryable at the task-framework boundary. Refusing to
skip is not an immediate terminal task error: final status and delay depend on the
configured framework retry budget. Recovery completion still probes nodes via JDBC;
probe caching/rate limiting remains a follow-up.

## Validation

Run the focused unit regressions from the repository root:

```sh
mvn -o -q -pl connectors-common/postgres-core,connectors/postgres-connector -am \
  -Dtest=PhysicalWalLogMinerTest,PostgresConnectorTest -DfailIfNoTests=false test
```

EFM acceptance still requires an environment test: continuously write uniquely
identified rows, promote the standby currently serving CDC with one planned
switchover, keep writing through the transition, and compare committed source rows
with the target. Capture each node's role/timeline, requested and confirmed offsets,
per-node WAL failures, and the eventual recovery-complete message. Also test a real
WAL gap with unsafe resume disabled: no skipped-range success message is allowed.
After recovery completion, also verify planned handoff with no further writes:
CDC must settle on a healthy standby and remain there for at least two 30-second
checker intervals, without another primary-first cycle. Repeat with ongoing writes.
