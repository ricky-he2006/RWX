"""Image build time: baseline memo for the TPC executor tests, from the pinned commit.

usage: python buildmemo.py <out.pkl>
Records, while running the tests once on the unmodified checkout:
  - the duckdb reference dataframe of every reference query (keyed by the duckdb sql text)
  - a digest of every query's optimized expression
  - which sqlglot lines run outside parse/optimize/transpile ("rlines")
A later run may skip executing a query whose optimized expression and reference sql are
unchanged, provided the patch touches none of the rlines.
"""
import os
import pickle
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import runner  # noqa: E402

IDS = [
    "tests/test_executor.py::TestExecutor::test_execute_tpcds",
    "tests/test_executor.py::TestExecutor::test_execute_tpch",
    "tests/test_executor.py::TestExecutor::test_optimized_tpch",
]
runner.zygote_import({"tests.test_executor"})
import sqlglot.executor as E  # noqa: E402
import tests.test_executor as T  # noqa: E402

REPO = os.path.realpath(runner.REPO) + os.sep
rec = {"pool_results": {}, "duck": {}, "dsql": {}, "opt": {}, "rlines": {}, "tpch": {}}
runner.MEMO_REC = rec
runner.MEMO_IDS = dict(enumerate(IDS))
runner.SUBMIT_LAZY = {0, 1}

depth = [0]


def front(f):
    def w(*a, **k):
        depth[0] += 1
        try:
            return f(*a, **k)
        finally:
            depth[0] -= 1
    return w


runner.install_memo()
E.optimize = front(E.optimize)
E.optimize._br_memo = True
T.transpile = front(T.transpile)
T.parse_one = front(T.parse_one)

mon = sys.monitoring
TOOL = 4
mon.use_tool_id(TOOL, "brmemo")
cur = {}


def on_line(code, line):
    fn = code.co_filename
    if not fn.startswith(REPO) or "/tests/" in fn[len(REPO) - 1:]:
        return mon.DISABLE
    if depth[0]:
        return None
    cur.setdefault(fn[len(REPO):], set()).add(line)
    return mon.DISABLE


mon.register_callback(TOOL, mon.events.LINE, on_line)
for i, tid in enumerate(IDS):
    cur = {}
    rec["tpch"][tid] = "tpch" in tid
    mon.restart_events()
    mon.set_events(TOOL, mon.events.LINE)
    failed = runner.run_tests([i], IDS, timeouts=[3000.0])
    mon.set_events(TOOL, 0)
    if failed:
        print("baseline test failed:", tid, file=sys.stderr)
        sys.exit(1)
    rec["rlines"][tid] = {f: frozenset(v) for f, v in cur.items()}
    print(tid, "queries", len(rec["opt"].get(tid, {})), "rlines", sum(len(v) for v in cur.values()), file=sys.stderr)
print("duck frames", len(rec["duck"]), file=sys.stderr)
with open(sys.argv[1], "wb") as fh:
    pickle.dump(rec, fh, protocol=4)
lite = {"opt": {k: bool(v) for k, v in rec["opt"].items()}, "rlines": rec["rlines"], "tpch": rec["tpch"]}
with open(sys.argv[1].replace(".pkl", "_lite.pkl"), "wb") as fh:
    pickle.dump(lite, fh, protocol=4)

