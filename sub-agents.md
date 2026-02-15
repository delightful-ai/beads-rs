# Sub-Agent Tracking

## Active/Completed Agents

- id: `019c541f-e02e-7363-859c-a5a3a3aad35e`
  scope: `bd show` non-JSON latency root cause and single-RPC design
  status: completed
  summary: Found `bd show` human path doing 4 IPC round trips (`show` + `deps` + `notes` + `list`) and proposed a single aggregated request/response path.

- id: `019c541f-e03d-7ad3-952b-93d0019a04d7`
  scope: repeated store identity resolution overhead
  status: completed
  summary: Found repeated verification/open work in store discovery despite cached resolutions; proposed verified-short-circuit caching.

- id: `019c541f-e052-7921-80ea-bc5f0ae10756`
  scope: backup ref lock contention / stale lock behavior
  status: completed
  summary: Found stale `.lock` ref files can wedge backup ref creation/prune and suggested locked-error cleanup+retry handling.

- id: `019c541f-e06e-76c2-803c-9ea7c8d476ac`
  scope: checkpoint decode warning (`WireLabelStateV1` invalid length)
  status: completed
  summary: Root cause is legacy labels array compatibility during checkpoint import; compatibility deserializer plus regression test has been added in current local changes.

- id: `019c541f-e098-73e1-b09d-28bf8e405137`
  scope: instrumentation gaps in `bd admin metrics`
  status: completed
  summary: Found no per-request IPC latency metrics emitted today; proposed request-type histogram hooks in daemon request dispatch.

- id: `019c544c-bb46-7a73-b428-522a75eaa6fa`
  scope: `bd-mtw` rare `ready` outlier root cause
  status: completed
  summary: Read-gate waiters can sit behind sync debounce and loop housekeeping; recommended immediate sync start on unsatisfied read gate and prioritizing read-gate waiter service.

- id: `019c544f-beb1-7221-ac0c-4beab7ed42d7`
  scope: `bd-hf5d` label add/remove tail latency root cause
  status: completed
  summary: Label tails come from synchronous WAL fsync path plus occasional checkpoint overlap; recommended reducing inline checkpoint contention and improving tail-focused instrumentation.

- id: `019c54fa-a983-71f1-bd1f-06d8c5808d9b`
  scope: working-copy bug review (sync/metrics/docs changes)
  status: completed
  summary: No correctness regressions found in backup-ref lock/metrics/docs changes; noted residual best-effort risk if lock metadata/PID checks repeatedly fail.

- id: `019c54fa-a995-7fd1-8ff6-10b0dba1ddc7`
  scope: working-copy test coverage audit
  status: completed
  summary: Flagged missing coverage around lock-contention metrics and cleanup-policy observability; recommended sync-level metric assertions/tests.

- id: `019c54fa-a9af-7bb1-a74b-917a412620b1`
  scope: benchmark/instrumentation methodology audit
  status: completed
  summary: Verified non-JSON hotpath harness and suggested provenance metadata capture (branch/commit/dirty/tool versions) for reproducibility.

## Reports

- saved: `sub-agent-reports.md`

## Work Plan Linkage

- Baseline benchmark: completed
- Implement single-RPC show path: completed
- Implement request-latency instrumentation: completed
- Implement store identity verified-cache short-circuit: completed
- Re-benchmark and compare: completed
- Backup lock stale-lock recovery: completed
- Daemon stale `store.lock` auto-reclaim (dead PID): completed
