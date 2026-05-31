import json
import statistics
from pathlib import Path
from time import perf_counter

import matplotlib.pyplot as plt
import psycopg2


DB_CONFIG = {
    "host": "localhost",
    "port": "15432",
    "dbname": "vietjobs",
    "user": "postgres",
    "password": "postgres",
}

WARMUP_RUNS = 1
MEASURED_RUNS = 5

BENCHMARKS = [
    {
        "algorithm": "Hash Join",
        "file": "01_hash_join.sql",
        "workload": "Real-time analytics",
        "capability": "Co-located distributed joins",
        "sql": "SELECT * FROM hash_join_search(2, U&'H\\00E0 N\\1ED9i', 50);",
    },
    {
        "algorithm": "Query Routing",
        "file": "02_query_routing.sql",
        "workload": "Multi-tenant / CRUD",
        "capability": "Shard pruning / router planner",
        "sql": "SELECT * FROM route_jobs_by_group(2, U&'H\\00E0 N\\1ED9i', 50);",
    },
    {
        "algorithm": "Scatter-Gather",
        "file": "02_query_routing.sql",
        "workload": "Real-time analytics",
        "capability": "Parallel distributed SELECT",
        "sql": "SELECT * FROM search_jobs_keyword('marketing');",
    },
    {
        "algorithm": "MapReduce Aggregation",
        "file": "03_mapreduce_agg.sql",
        "workload": "Real-time analytics",
        "capability": "Worker partial aggregate and coordinator merge",
        "sql": "SELECT * FROM mapreduce_salary_stats();",
    },
    {
        "algorithm": "Semi-Join",
        "file": "04_semi_join.sql",
        "workload": "Real-time analytics",
        "capability": "Co-located semi join",
        "sql": "SELECT * FROM semi_join_jobs_with_skill('Python', 2, U&'H\\00E0 N\\1ED9i');",
    },
    {
        "algorithm": "Parallel Top-K",
        "file": "05_parallel_topk.sql",
        "workload": "Data warehousing",
        "capability": "LIMIT pushdown and coordinator merge",
        "sql": "SELECT * FROM topk_salary_global(10);",
    },
]


def fetch_all(cursor, sql):
    cursor.execute(sql)
    if cursor.description:
        cursor.fetchall()


def time_query(cursor, sql):
    start = perf_counter()
    fetch_all(cursor, sql)
    return (perf_counter() - start) * 1000


def percentile(values, pct):
    values = sorted(values)
    index = (len(values) - 1) * pct
    lower = int(index)
    upper = min(lower + 1, len(values) - 1)
    weight = index - lower
    return values[lower] * (1 - weight) + values[upper] * weight


def benchmark_case(cursor, case):
    for _ in range(WARMUP_RUNS):
        fetch_all(cursor, case["sql"])

    times = []
    for _ in range(MEASURED_RUNS):
        times.append(time_query(cursor, case["sql"]))

    mean_ms = statistics.mean(times)
    return {
        **case,
        "warmup_runs": WARMUP_RUNS,
        "measured_runs": MEASURED_RUNS,
        "times_ms": [round(v, 3) for v in times],
        "mean_ms": round(mean_ms, 3),
        "median_ms": round(statistics.median(times), 3),
        "p95_ms": round(percentile(times, 0.95), 3),
        "min_ms": round(min(times), 3),
        "max_ms": round(max(times), 3),
        "qps": round(1000 / mean_ms, 3),
    }


def write_json(results, output_path):
    payload = {
        "method": {
            "source": "Citus SIGMOD 2021 benchmark style",
            "rule": "one warm-up run, five measured runs, mean, p95, throughput, speedup",
        },
        "db": DB_CONFIG | {"password": "***"},
        "results": results,
    }
    output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")


def write_plot(results, output_path):
    labels = [row["algorithm"] for row in results]
    mean = [row["mean_ms"] for row in results]
    p95 = [row["p95_ms"] for row in results]
    x_axis = range(len(results))

    plt.figure(figsize=(11, 6))
    plt.bar(x_axis, mean, label="Mean latency", color="#4e79a7")
    plt.plot(x_axis, p95, marker="o", label="p95 latency", color="#e15759")
    plt.xticks(x_axis, labels, rotation=25, ha="right")
    plt.ylabel("Milliseconds")
    plt.title("VietJobs Citus Query Benchmark")
    plt.legend()
    plt.tight_layout()
    plt.savefig(output_path, dpi=160)
    plt.close()


def main():
    root = Path(__file__).resolve().parent
    results_dir = root / "results"
    plots_dir = root / "plots"
    results_dir.mkdir(exist_ok=True)
    plots_dir.mkdir(exist_ok=True)

    results = []
    with psycopg2.connect(**DB_CONFIG) as conn:
        conn.autocommit = True
        with conn.cursor() as cursor:
            for case in BENCHMARKS:
                result = benchmark_case(cursor, case)
                results.append(result)
                print(
                    f"{result['algorithm']}: "
                    f"mean={result['mean_ms']:.2f} ms, "
                    f"p95={result['p95_ms']:.2f} ms, "
                    f"qps={result['qps']:.2f}"
                )

    slowest = max(row["mean_ms"] for row in results)
    for row in results:
        row["speedup"] = round(slowest / row["mean_ms"], 3)

    write_json(results, results_dir / "benchmark.json")
    write_plot(results, plots_dir / "benchmark.png")


if __name__ == "__main__":
    main()
