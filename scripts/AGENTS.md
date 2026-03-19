## Boundary
This directory holds side-effectful and operator-facing shell tooling for install, profiling, fixture generation, and soak/grind flows.
NEVER: assume a script here is side-effect free.

## Routing
- Copy `profile-tests.sh` or `profile-hotpaths.sh` for artifact-producing helpers with explicit env knobs and deterministic `tmp/perf/*` outputs.
- Copy `install.sh` for network/download/checksum flows.
- Do not copy `grind-p2.sh` unless you really want an infinite operator loop.
- `generate-v0_1_26-store-fixture.sh` is the checked-in fixture-generation pattern when a script must emit repo artifacts.

## Local rules
- Keep required commands and env knobs explicit near the top of the script.
- Preserve documented output locations and artifact layout when a script writes under `tmp/`.
- Copy the explicit env/output structure from `profile-tests.sh` or `profile-hotpaths.sh`; do not copy the endless operator-loop shape from `grind-p2.sh` unless that is truly the job.
- Treat `install.sh` as a networked/release-facing script: verify pinned download/checksum paths in an isolated temp dir instead of piping it straight to a live shell.
- Treat `sync-soak.sh` and `grind-p2.sh` as operator loops, not routine verification helpers.
- Safe default proof loops differ by family: profiling/fixture scripts can usually be run directly, `install.sh` should be exercised in temp-dir isolation, `sync-soak.sh` should be checked with explicit temp roots and bounded inputs, and `grind-p2.sh` should usually be validated by inputs/paths rather than by launching the full infinite loop.
- Re-run edited profiling or fixture scripts directly; for networked or operator-loop scripts, validate inputs, commands, and temp/output paths without blindly executing the full loop.
