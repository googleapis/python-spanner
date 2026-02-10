#!/usr/bin/env python3
"""
Multi-Process Pool Comparison Benchmark for Cloud Spanner + SQLAlchemy

Compares different connection/session pooling strategies when using
Cloud Spanner with SQLAlchemy in a multi-process setup (e.g. gunicorn workers).

Key insight: The default SQLAlchemy + Spanner integration creates a separate
PingingPool (with BatchCreateSessions RPC) per DBAPI connection, leading to
N_connections x M_sessions_per_pool total sessions and high setup overhead.
The "shared pool" scenario demonstrates passing a pre-existing Database object
to connect(), so all DBAPI connections share a single PingingPool — matching
the raw Spanner client's architecture.

Requires the `database` parameter patch on spanner_dbapi.connect().

Usage:
    # Set environment variables for your Spanner instance
    export SPANNER_PROJECT_ID=my-project
    export SPANNER_INSTANCE_ID=my-instance
    export SPANNER_DATABASE_ID=my-database

    # Run all scenarios with 3 runs (default)
    python benchmark_multiprocess_pools.py

    # Run only specific scenarios
    python benchmark_multiprocess_pools.py --scenarios shared raw

    # Run with 1 iteration for quick testing
    python benchmark_multiprocess_pools.py --runs 1

    # Run in fixed order (no randomization)
    python benchmark_multiprocess_pools.py --no-randomize

Scenarios tested:
- 5x5:     QueuePool(5) x PingingPool(5)  = 25 sessions/process (balanced)
- 1x25:    QueuePool(1) x PingingPool(25) = 25 sessions/process (serialized)
- 25x1:    QueuePool(25) x PingingPool(1) = 25 sessions/process (many conns)
- 5xdefault: QueuePool(5) x default pool
- default: All SQLAlchemy/Spanner defaults
- static:  StaticPool x PingingPool(25)
- shared:  NullPool + single shared PingingPool(25) per process  <-- NEW
- raw:     Raw Spanner client with PingingPool(25), no SQLAlchemy
"""

import os
import sys

# Validate required environment variables
REQUIRED_ENV = ['SPANNER_PROJECT_ID', 'SPANNER_INSTANCE_ID', 'SPANNER_DATABASE_ID']
missing = [v for v in REQUIRED_ENV if not os.environ.get(v)]
if missing:
    print(f"Error: Missing required environment variables: {', '.join(missing)}")
    print(f"\nSet them before running:")
    print(f"  export SPANNER_PROJECT_ID=my-project")
    print(f"  export SPANNER_INSTANCE_ID=my-instance")
    print(f"  export SPANNER_DATABASE_ID=my-database")
    sys.exit(1)

os.environ['GOOGLE_CLOUD_SPANNER_MULTIPLEXED_SESSIONS'] = 'false'

import warnings
warnings.filterwarnings(
    "ignore",
    message="This method is non-operational as a transaction has not been started",
    category=UserWarning,
    module="google.cloud.sqlalchemy_spanner",
)

import time
import multiprocessing
from concurrent.futures import ThreadPoolExecutor
import logging
import argparse
import statistics
import random

from google.cloud import spanner
from google.cloud.spanner_dbapi import connect
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool, StaticPool, NullPool

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d [PID:%(process)d] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)
logging.getLogger('alembic').setLevel(logging.WARNING)

# Configuration
QUERIES_PER_PROCESS = 50
NUM_PROCESSES = 4
QUERY_DELAY_MS = 50

project_id = os.environ['SPANNER_PROJECT_ID']
instance_id = os.environ['SPANNER_INSTANCE_ID']
database_id = os.environ['SPANNER_DATABASE_ID']


def create_spanner_connection(session_pool_size=None, client=None):
    """Create Spanner DBAPI connection with configurable PingingPool size."""
    if session_pool_size is None:
        conn = connect(instance_id, database_id, client=client)
    else:
        pool = spanner.PingingPool(size=session_pool_size, default_timeout=30, ping_interval=3000)
        conn = connect(
            instance_id=instance_id, database_id=database_id,
            project=project_id, pool=pool, client=client,
        )
    return conn


def run_query_raw_spanner(snapshot, query_id, process_id):
    """Execute a single query using raw Spanner client."""
    start_time = time.time()
    try:
        exec_start = time.time()
        results = snapshot.execute_sql("SELECT 1 as query_id")
        list(results)[0]
        exec_end = time.time()

        session_id = getattr(snapshot._session, 'session_id', None) if hasattr(snapshot, '_session') else None
        total_time = (exec_end - start_time) * 1000
        exec_ms = (exec_end - exec_start) * 1000

        return {
            'query_id': query_id, 'process_id': process_id, 'success': True,
            'start_time': start_time, 'end_time': exec_end,
            'checkout_time_abs': start_time,
            'total_time': total_time, 'checkout_time': 0, 'exec_time': exec_ms,
            'session_id': session_id,
        }
    except Exception as e:
        exec_end = time.time()
        return {
            'query_id': query_id, 'process_id': process_id, 'success': False,
            'start_time': start_time, 'end_time': exec_end,
            'error': str(e), 'total_time': (exec_end - start_time) * 1000,
        }


def run_query(engine, query_id, process_id):
    """Execute a single query via SQLAlchemy engine."""
    start_time = time.time()
    try:
        with engine.connect() as conn:
            checkout_time = time.time()
            exec_start = time.time()
            result = conn.execute(text("SELECT 1 as query_id"))
            result.fetchone()
            exec_end = time.time()

            underlying_conn = conn.connection
            dbapi_conn = underlying_conn.connection if hasattr(underlying_conn, 'connection') else underlying_conn

            session_id = None
            if hasattr(dbapi_conn, '_snapshot') and dbapi_conn._snapshot and hasattr(dbapi_conn._snapshot, '_session'):
                session_id = getattr(dbapi_conn._snapshot._session, 'session_id', None)

            return {
                'query_id': query_id, 'process_id': process_id, 'success': True,
                'start_time': start_time, 'end_time': exec_end,
                'checkout_time_abs': checkout_time,
                'total_time': (exec_end - start_time) * 1000,
                'checkout_time': (checkout_time - start_time) * 1000,
                'exec_time': (exec_end - exec_start) * 1000,
                'sqlalchemy_conn_id': id(conn),
                'dbapi_conn_id': id(dbapi_conn),
                'session_id': session_id,
            }
    except Exception as e:
        exec_end = time.time()
        return {
            'query_id': query_id, 'process_id': process_id, 'success': False,
            'start_time': start_time, 'end_time': exec_end,
            'error': str(e), 'total_time': (exec_end - start_time) * 1000,
        }


def worker_process_raw_spanner(process_id, session_pool_size, num_queries, queue):
    """Worker using raw Spanner client (no SQLAlchemy)."""
    try:
        _t_worker_start = time.time()

        _t0 = time.time()
        client = spanner.Client(project=project_id)
        _t1 = time.time()
        logger.info(f"[Process {process_id}] spanner.Client(): {(_t1-_t0)*1000:.1f}ms")

        instance = client.instance(instance_id)
        pool = spanner.PingingPool(size=session_pool_size, default_timeout=30)

        _t2 = time.time()
        database = instance.database(database_id, pool=pool)
        _t3 = time.time()
        logger.info(f"[Process {process_id}] instance.database() (incl pool.bind): {(_t3-_t2)*1000:.1f}ms")

        _t_setup_done = time.time()
        logger.info(f"[Process {process_id}] TOTAL SETUP: {(_t_setup_done-_t_worker_start)*1000:.1f}ms")

        def run_with_snapshot(q):
            with database.snapshot() as snapshot:
                return run_query_raw_spanner(snapshot, q, process_id)

        results = []
        _t_queries_start = time.time()
        with ThreadPoolExecutor(max_workers=num_queries) as executor:
            futures = [executor.submit(run_with_snapshot, q) for q in range(num_queries)]
            for future in futures:
                results.append(future.result())
        _t_queries_done = time.time()
        logger.info(f"[Process {process_id}] ALL QUERIES: {(_t_queries_done-_t_queries_start)*1000:.1f}ms")
        logger.info(f"[Process {process_id}] WORKER TOTAL: {(_t_queries_done-_t_worker_start)*1000:.1f}ms")

        queue.put({'process_id': process_id, 'results': results})
    except Exception as e:
        queue.put({'process_id': process_id, 'error': str(e)})


def worker_process(process_id, pool_type, pool_size, session_pool_size, num_queries, queue):
    """Worker using SQLAlchemy engine with various pool configurations."""
    try:
        _t_worker_start = time.time()
        conn_string = f"spanner+spanner:///projects/{project_id}/instances/{instance_id}/databases/{database_id}"

        _t0 = time.time()
        shared_client = spanner.Client(project=project_id)
        _t1 = time.time()
        logger.info(f"[Process {process_id}] spanner.Client(): {(_t1-_t0)*1000:.1f}ms")

        _t2 = time.time()
        if pool_type == 'shared_pool':
            # SHARED POOL: Single PingingPool shared by all SQLAlchemy connections.
            # Uses the `database` parameter on connect() to bypass per-connection
            # pool creation. SQLAlchemy NullPool creates lightweight DBAPI wrappers.
            instance = shared_client.instance(instance_id)
            shared_db_pool = spanner.PingingPool(
                size=session_pool_size, default_timeout=30, ping_interval=3000
            )
            _t_db_start = time.time()
            shared_database = instance.database(database_id, pool=shared_db_pool)
            _t_db_done = time.time()
            logger.info(f"[Process {process_id}] Shared PingingPool({session_pool_size}) + Database setup: {(_t_db_done-_t_db_start)*1000:.1f}ms")

            engine = create_engine(
                conn_string, poolclass=NullPool, pool_reset_on_return=None,
                creator=lambda: connect(
                    instance_id, database_id, project=project_id,
                    database=shared_database, client=shared_client
                ),
                isolation_level="AUTOCOMMIT",
            )
        elif pool_type == 'default':
            engine = create_engine(
                conn_string, pool_reset_on_return=None,
                creator=lambda: create_spanner_connection(session_pool_size, client=shared_client),
                isolation_level="AUTOCOMMIT",
            )
        elif pool_type == 'static':
            engine = create_engine(
                conn_string, poolclass=StaticPool, pool_reset_on_return=None,
                creator=lambda: create_spanner_connection(session_pool_size, client=shared_client),
                isolation_level="AUTOCOMMIT",
            )
        else:
            engine = create_engine(
                conn_string, poolclass=QueuePool,
                pool_size=pool_size, max_overflow=0, pool_timeout=60,
                pool_pre_ping=False, pool_reset_on_return=None,
                creator=lambda: create_spanner_connection(session_pool_size, client=shared_client),
                isolation_level="AUTOCOMMIT",
            )
        _t3 = time.time()
        logger.info(f"[Process {process_id}] create_engine(): {(_t3-_t2)*1000:.1f}ms")

        _t_setup_done = time.time()
        logger.info(f"[Process {process_id}] TOTAL SETUP: {(_t_setup_done-_t_worker_start)*1000:.1f}ms")

        results = []
        _t_queries_start = time.time()
        with ThreadPoolExecutor(max_workers=num_queries) as executor:
            futures = [executor.submit(run_query, engine, q, process_id) for q in range(num_queries)]
            for future in futures:
                results.append(future.result())
        _t_queries_done = time.time()
        logger.info(f"[Process {process_id}] ALL QUERIES: {(_t_queries_done-_t_queries_start)*1000:.1f}ms")
        logger.info(f"[Process {process_id}] WORKER TOTAL: {(_t_queries_done-_t_worker_start)*1000:.1f}ms")

        queue.put({'process_id': process_id, 'results': results})
        engine.dispose()
    except Exception as e:
        queue.put({'process_id': process_id, 'error': str(e)})


def run_raw_spanner_test(session_pool_size):
    """Run benchmark with raw Spanner client."""
    logger.info(f"Starting raw Spanner test: {NUM_PROCESSES} processes, {session_pool_size} sessions each")
    start_time = time.time()
    result_queue = multiprocessing.Queue()

    processes = []
    for i in range(NUM_PROCESSES):
        p = multiprocessing.Process(
            target=worker_process_raw_spanner,
            args=(i, session_pool_size, QUERIES_PER_PROCESS, result_queue)
        )
        p.start()
        processes.append(p)
    for p in processes:
        p.join()

    total_time = (time.time() - start_time) * 1000
    all_results = []
    for _ in range(NUM_PROCESSES):
        proc_result = result_queue.get()
        if 'error' in proc_result:
            logger.error(f"Process {proc_result['process_id']} failed: {proc_result['error']}")
        else:
            all_results.extend(proc_result['results'])

    return _analyze_results(all_results, total_time, 'raw_spanner', None, session_pool_size)


def run_pool_configuration_test(pool_type, pool_size=None, session_pool_size=None):
    """Test a specific pool configuration across multiple processes."""
    pool_label = "Default (5)" if pool_size is None else str(pool_size)
    session_label = "Default" if session_pool_size is None else str(session_pool_size)

    logger.info(f"\n{'='*80}")
    logger.info(f"Testing: {pool_type.title()} Pool {pool_label}x{session_label}")
    logger.info(f"{'='*80}")
    logger.info(f"  Processes: {NUM_PROCESSES}, Queries/process: {QUERIES_PER_PROCESS}")

    start_time = time.time()
    result_queue = multiprocessing.Queue()

    processes = []
    for i in range(NUM_PROCESSES):
        p = multiprocessing.Process(
            target=worker_process,
            args=(i, pool_type, pool_size, session_pool_size, QUERIES_PER_PROCESS, result_queue)
        )
        p.start()
        processes.append(p)
    for p in processes:
        p.join()

    total_time = (time.time() - start_time) * 1000
    all_results = []
    while not result_queue.empty():
        proc_result = result_queue.get()
        if 'error' in proc_result and 'results' not in proc_result:
            logger.error(f"Process {proc_result['process_id']} failed: {proc_result['error']}")
        else:
            all_results.extend(proc_result['results'])

    return _analyze_results(all_results, total_time, pool_type, pool_size, session_pool_size)


def _analyze_results(all_results, total_time, pool_type, pool_size, session_pool_size):
    """Analyze and print benchmark results."""
    successful = [r for r in all_results if r['success']]
    failed = [r for r in all_results if not r['success']]

    total_queries = len(all_results)
    success_rate = len(successful) / total_queries * 100 if total_queries > 0 else 0

    query_times = [r['total_time'] for r in successful]
    if query_times:
        median_qt = statistics.median(query_times)
        min_qt = min(query_times)
        max_qt = max(query_times)
    else:
        median_qt = min_qt = max_qt = 0

    all_sessions = set(r.get('session_id') for r in successful if r.get('session_id'))
    all_dbapi_conns = set(r.get('dbapi_conn_id') for r in successful if r.get('dbapi_conn_id'))

    label = f"Raw Spanner ({session_pool_size} sessions)" if pool_type == 'raw_spanner' else f"{pool_type} {pool_size}x{session_pool_size}"
    logger.info(f"\n{'='*80}")
    logger.info(f"RESULTS: {label}")
    logger.info(f"{'='*80}")
    logger.info(f"  Success: {len(successful)}/{total_queries} ({success_rate:.1f}%)")
    if failed:
        for f in failed[:5]:
            logger.info(f"  FAILED: Q{f['query_id']} P{f['process_id']}: {f.get('error', '?')}")
    logger.info(f"  Total time: {total_time:.0f}ms")
    logger.info(f"  Query latency: median={median_qt:.0f}ms, min={min_qt:.0f}ms, max={max_qt:.0f}ms")
    logger.info(f"  Unique sessions: {len(all_sessions)}")
    if all_dbapi_conns:
        logger.info(f"  Unique DBAPI conns: {len(all_dbapi_conns)}")

    return {
        'pool_type': pool_type,
        'pool_size': pool_size,
        'session_pool_size': session_pool_size,
        'success_rate': success_rate,
        'total_time': total_time,
        'median_latency': median_qt,
        'min_latency': min_qt,
        'max_latency': max_qt,
        'total_sessions': len(all_sessions),
        'total_dbapi_conns': len(all_dbapi_conns),
    }


def main():
    parser = argparse.ArgumentParser(
        description='Multi-Process Pool Comparison Benchmark for Cloud Spanner + SQLAlchemy',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Available scenarios:
  5x5       - QueuePool 5x5 (Balanced)
  1x25      - QueuePool 1x25 (Minimal Connections)
  25x1      - QueuePool 25x1 (Maximum Connections)
  5xdefault - QueuePool 5xDefault
  default   - All Defaults
  static    - StaticPool x 25
  shared    - NullPool + Shared PingingPool(25)  <-- uses database= param
  raw       - Raw Spanner client x 25 (no SQLAlchemy)

Examples:
  python benchmark_multiprocess_pools.py --scenarios shared raw --runs 1
  python benchmark_multiprocess_pools.py --runs 3
        """
    )
    parser.add_argument(
        '--scenarios', nargs='+',
        choices=['5x5', '1x25', '25x1', '5xdefault', 'default', 'static', 'shared', 'raw', 'all'],
        default=['all'], help='Scenarios to run (default: all)'
    )
    parser.add_argument('--runs', type=int, default=3, help='Runs per scenario (default: 3)')
    parser.add_argument('--no-randomize', action='store_true', help='Fixed order instead of randomized')

    args = parser.parse_args()

    logger.info("=" * 80)
    logger.info("MULTI-PROCESS POOL COMPARISON BENCHMARK")
    logger.info("=" * 80)
    logger.info(f"Spanner: {project_id}/{instance_id}/{database_id}")
    logger.info(f"Config: {NUM_PROCESSES} processes, {QUERIES_PER_PROCESS} queries each, {args.runs} run(s)")

    all_scenarios = {
        '5x5':       {'name': 'QueuePool 5x5',       'type': 'pool', 'pool_type': 'queuepool', 'pool_size': 5,  'session_pool_size': 5},
        '1x25':      {'name': 'QueuePool 1x25',      'type': 'pool', 'pool_type': 'queuepool', 'pool_size': 1,  'session_pool_size': 25},
        '25x1':      {'name': 'QueuePool 25x1',      'type': 'pool', 'pool_type': 'queuepool', 'pool_size': 25, 'session_pool_size': 1},
        '5xdefault': {'name': 'QueuePool 5xDefault',  'type': 'pool', 'pool_type': 'queuepool', 'pool_size': 5,  'session_pool_size': None},
        'default':   {'name': 'Default',              'type': 'pool', 'pool_type': 'default',   'pool_size': None, 'session_pool_size': None},
        'static':    {'name': 'StaticPool x25',       'type': 'pool', 'pool_type': 'static',    'pool_size': 1,  'session_pool_size': 25},
        'shared':    {'name': 'SharedPool x25',       'type': 'pool', 'pool_type': 'shared_pool', 'pool_size': None, 'session_pool_size': 25},
        'raw':       {'name': 'Raw Spanner x25',      'type': 'raw',  'session_pool_size': 25},
    }

    if 'all' in args.scenarios:
        scenarios = list(all_scenarios.values())
    else:
        scenarios = [all_scenarios[key] for key in args.scenarios]

    logger.info(f"Scenarios: {', '.join(s['name'] for s in scenarios)}\n")

    all_run_results = {s['name']: [] for s in scenarios}

    for run_num in range(1, args.runs + 1):
        logger.info(f"\n{'='*80}")
        logger.info(f"RUN {run_num}/{args.runs}")
        logger.info(f"{'='*80}")

        run_scenarios = scenarios if args.no_randomize else random.sample(scenarios, len(scenarios))
        for i, scenario in enumerate(run_scenarios, 1):
            logger.info(f"  {i}. {scenario['name']}")

        for i, scenario in enumerate(run_scenarios, 1):
            logger.info(f"\n--- {scenario['name']} ({i}/{len(scenarios)}) ---")
            if scenario['type'] == 'raw':
                result = run_raw_spanner_test(session_pool_size=scenario['session_pool_size'])
            else:
                result = run_pool_configuration_test(
                    pool_type=scenario['pool_type'],
                    pool_size=scenario['pool_size'],
                    session_pool_size=scenario['session_pool_size'],
                )
            result['scenario_name'] = scenario['name']
            all_run_results[scenario['name']].append(result)

    # Build summary table sorted by median total time
    summary_rows = []
    for scenario in scenarios:
        results = all_run_results[scenario['name']]
        if not results:
            continue
        total_times = [r['total_time'] for r in results]
        median_latencies = [r['median_latency'] for r in results]
        min_latencies = [r['min_latency'] for r in results]
        max_latencies = [r['max_latency'] for r in results]
        summary_rows.append({
            'name': scenario['name'],
            'success': f"{statistics.mean(r['success_rate'] for r in results):.0f}%",
            'total_time': statistics.median(total_times),
            'total_range': f"{min(total_times):.0f}-{max(total_times):.0f}" if len(total_times) > 1 else "",
            'med_latency': statistics.median(median_latencies),
            'min_latency': statistics.median(min_latencies),
            'max_latency': statistics.median(max_latencies),
            'sessions': results[0]['total_sessions'],
            'dbapi_conns': results[0]['total_dbapi_conns'],
        })
    summary_rows.sort(key=lambda r: r['total_time'])

    # Print summary table
    col_w = 110
    logger.info(f"\n{'='*col_w}")
    logger.info(f"RESULTS SUMMARY ({args.runs} run{'s' if args.runs > 1 else ''}, ranked by total time)")
    logger.info(f"{'='*col_w}")
    logger.info(f"{'#':>2}  {'Scenario':<22}  {'Success':>7}  {'Total (ms)':>14}  {'Med':>6}  {'Min':>6}  {'Max':>6}  {'Sessions':>8}  {'DBAPI':>5}")
    logger.info(f"{'-'*col_w}")
    for rank, row in enumerate(summary_rows, 1):
        dbapi_str = str(row['dbapi_conns']) if row['dbapi_conns'] else "-"
        total_str = f"{row['total_time']:.0f}"
        if row['total_range']:
            total_str += f" ({row['total_range']})"
        logger.info(
            f"{rank:>2}. {row['name']:<22}  {row['success']:>7}  {total_str:>14}  "
            f"{row['med_latency']:>5.0f}  {row['min_latency']:>5.0f}  {row['max_latency']:>5.0f}  "
            f"{row['sessions']:>8}  {dbapi_str:>5}"
        )
    logger.info(f"{'='*col_w}")
    logger.info("DONE")


if __name__ == '__main__':
    main()
