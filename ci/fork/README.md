# Fork CI

Upstream workflows run on ClickHouse's self-hosted runners and never start in a fork.
`.github/workflows/fork_dev.yml` builds and tests on free GitHub-hosted runners instead.

- Trigger: a push to a `dev/**` branch, or a manual run once this file is on the default branch.
- Build: `ci/fork/build.sh` in `clickhouse/fasttest`, Fast test's feature set plus S3, Avro and Parquet.
  A cold build takes more than one 6 h job, so up to three stages resume from the sccache kept in the Actions cache.
  A binary is cached by source hash: a push that changes only tests or workflows skips the build.
- Tests: upstream's `functional_tests.py` in local-run mode on the built binary.
  Patterns default to `iceberg distributed_plan`; a commit message line `tests: <patterns>` overrides them.
- Artifacts: `clickhouse` (zstd binary) and `test-logs`, kept 7 days.

Do not open pull requests inside the fork: `pull_request.yml` would queue jobs for runners that do not exist.
Drop `ci/fork/` and both `fork_dev*.yml` from a branch before proposing it upstream.
