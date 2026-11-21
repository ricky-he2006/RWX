# Blast Radius: test selection for sqlglot patches

An entry for the RWX **Blast Radius** track. For every patch against sqlglot `eb11aec`,
it prints which tests to run and how to split them over up to 8 workers.

Nothing is looked up by patch. Every decision comes from the patch text and from maps
built from the pinned commit.

## How it decides

1. **Static reach** (`app/selcore.py`, `app/patchlib.py`).
   - The patch is applied in memory and diffed at the AST level.
   - Comment-only or formatting-only edits select nothing.
   - Each changed statement is classified as runtime (inside a function body), import-time, signature, or annotation.
   - Candidate tests come from a per-test line-coverage map of the pinned commit (`app/map.json.gz`). This covers test phases, class setup, pickled pool work and import-time lines.
   - Fixture and data files map to the tests that open them.
   - Module adds or removes, binary files and unparsable patches select everything.
2. **Running the candidates cheaply** (`app/runner.py`, `app/selector.py`).
   - A zygote process imports sqlglot and all test modules once. Each commit then runs in a forked child.
   - Runtime-only edits are applied by swapping `__code__` on the live functions.
   - Edits that run at import time rebuild a symlink overlay and re-import. The old and new module and class state are then diffed structurally (`app/statediff.py`). Each changed attribute or dict key is mapped to the tests that read it, using an attribute-read trace of the pinned commit (`app/reads.json.gz`).
   - A cost model (`plan_split`) weighs running a test (container CPU) against selecting it unrun (scheduled seconds plus 2 s per worker). The expensive tail is selected without being run.
   - Runtime edits are instrumented with old/new differential probes. If the light tests pass and every probe saw identical values, the heavy tail is dropped.
   - TPC-H and TPC-DS execute only the queries whose coverage reaches the edited lines, and stop at the first failing query.
   - The image build runs the TPC executor tests once on the pinned checkout (`app/buildmemo.py`) and keeps the DuckDB reference results and a digest of every optimized query. At run time a reference result is reused when the DuckDB SQL text is unchanged. A query skips execution when its optimized expression is unchanged and the patch touches no code that runs after optimization.
   - A module global that changed is mapped to the tests that run a line naming it, or that live in a test file naming it.
   - *Modules that stop importing* (`app/imports.json.gz`, built by `tools/buildimports.py`): the map records which sqlglot modules each test imports, directly or lazily. Every test that imports a module the patch broke is selected without being run; the other tests are judged against the remaining state as usual.
   - Tests that clear a lot of schedule time per CPU second run first. After each new failure the remaining tests are re-planned, and only those that still clear more schedule time than they cost in CPU are run.
   - After the light tests, a heavy test that was left unrun is run after all when the schedule time it would free outweighs its CPU.
   - Every child has a deadline. If one does not finish, that commit falls back to the static candidate set, or to every test if the patch changes import-time state.
3. **Phase replay** (`app/replay.py`, built by `tools/buildreplay.py`).
   - A *phase* is one outermost `Dialect.parse` / `Dialect.parse_into` call (tokenize + parse) or one outermost `Dialect.generate` call. At the pinned commit every test's phase inputs were recorded (about 52,000 unique ones) with a digest of everything a caller can observe: the returned trees or SQL text, a raised error, and log records. A generate input is either the result of a recorded parse or an exact snapshot of the tree (`app/replay.snap`).
   - The same trace recorded which lines and class attributes each test touches *outside* phases, which functions each input enters, and which class attributes each input reads.
   - For a patch that leaves import-time state alone, or changes only class attributes with known readers, a candidate test that touches the change only inside phases cannot change its outcome unless one of its recorded inputs now produces a different output. The selector replays the relevant inputs under the patch and keeps only the tests with a changed output.
   - Only inputs that enter a changed function, or read a changed attribute, are replayed. One process-level cache in sqlglot (a generator class's dispatch table, built once from `TRANSFORMS` and the `*_sql` methods) hides those reads from every input but the first, so for a change to such an attribute every input that creates a generator counts as a reader. A patch to the code that fills that cache selects everything.
   - Digests ignore hash-seed-dependent ordering; inputs whose output still varies between interpreters are excluded at build time, along with the tests that use them.
   - `test_max_nodes` parses a query of one unit repeated 10,000 times. Its phase is checked with the same unit repeated 100 and 400 times and the node limit scaled by the same ratio. This is a heuristic stand-in, not a proof.
4. **Cheaper test runs.**
   - *Optimizer memo* (`app/optmemo.py`, built in the image by `app/buildopt.py`): `optimize()` is a fixed pipeline of pure rules. The build records, for every `optimize()` call of the optimizer and executor tests, a digest and an exact snapshot of the tree before each rule, and which lines each rule ever runs. Under a patch to function bodies, only the rules that can reach a changed line run, on their baseline inputs; if their output matches the baseline the stored final tree is returned.
   - *Fixture iterations* (`tools/builditer.py`): tests that loop over a fixture file run one case per iteration. Iterations that never reach a changed function body are skipped. A test is only eligible if it still passes at the pinned commit with half its iterations removed.
   - *Shared class fixtures*: `TestExecutor.setUpClass` (over a second of CSV parsing) runs once in the parent process. A commit reuses it only when no test file changed, nothing was re-imported, and no changed line is among the lines that setup ran. DuckDB connections are created lazily in each child from the statements recorded during that setup.
5. **Schedule** (`app/sched.py`). LPT packing over 1–8 workers, picking the count that minimises the busiest worker plus 2 s per worker.
6. **Staying alive** (`supervise` in `app/selector.py`).
   - The work runs in a child of a small supervisor. If the worker dies or stalls while analysing one patch, that commit is answered with every test and the work restarts without it. If it dies later, the static answers are printed. An answer line is always printed for every commit.
   - A test that exceeds its time limit under a patch counts as failed, and no further tests are run for that commit: code that hangs once usually hangs everywhere. The same applies to a replayed input that hangs. Each child also has a hard CPU limit.

## Layout

- `Dockerfile`: python 3.13-slim, the locked requirements, and sqlglot at `eb11aec` from GitHub.
- `app/`: the selector and its prebuilt maps. All maps were built from the pinned commit.
- `tools/`: scripts that build the maps:
  - `covplugin.py` and `buildmap.py`: coverage
  - `importcov.py`: import-time lines
  - `attrtrace.py` and `buildreads.py`: attribute reads
  - `percpu.py`: per-test CPU
  - `mknoise.py`: state-diff noise
  - `buildsub.py`: per-query coverage
  - `buildreplay.py`: phase inputs, digests, snapshots and out-of-phase coverage
  - `attrtrace.py` with `BR_PHASE=1`: attribute reads made outside phases, and per-input attribute reads
  - `builditer.py`: per-iteration coverage of fixture-driven tests
  - `buildimports.py`: the sqlglot modules each test imports
  - `optfrac.py`: share of each test's time spent in `optimize()`
  - `genmutants.py`, `fastlabel.py` and friends: evaluation on synthetic mutants
- `make sample` scores three fresh containers and reports the median of simulated time plus measured CPU. The default build targets linux/amd64. On ARM, use `make sample PLATFORM=linux/arm64 IMAGE=blast-radius-native` for native CPU measurements. AMD64 emulation can crash DuckDB during preparation and exercise a slower fallback. A native AMD64 Docker host gives the closest hardware comparison with the fleet.

The tools expect a dev container with this repo at `/w`, the sqlglot checkout at `/w/sqlglot`, and the starter kit at `/kit`.
