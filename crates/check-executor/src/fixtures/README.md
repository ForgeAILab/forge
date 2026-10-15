# Captured test failures

- `cargo-test.txt`: small verbatim excerpt from Forge's actual CI log at
  `/Volumes/Data/tmp/refactor/quarantine/gate-services-integration.log`.
- `vitest.txt`, `junit.xml`: Vitest 4.1.11, two deliberately failing tests,
  captured with `vitest run --reporter=verbose --reporter=junit`.
- `jest.txt`: Jest 30.5.2, the same two failing assertions, `jest --runInBand --colors`.
- `pytest.txt`: pytest verbose coloured output, two failing assertions,
  `pytest -v --color=yes --junitxml=results.xml` (uv's setup lines included).
- `go-test.txt`: `go test -v`, one parent test and two failing subtests.

Throwaway runner projects were created under `/Volumes/Data/tmp/guards-fixtures/`;
only these small captured outputs are checked in. Captured 2026-10-15.
