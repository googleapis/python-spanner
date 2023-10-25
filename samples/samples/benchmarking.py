import threading
from google.cloud import spanner_v1
import time
import math

from google.cloud.spanner_v1 import transaction, pool, _helpers
from google.cloud import spanner
import concurrent.futures

# Define your Spanner instance and database information
project_id = "your_project_id"
instance_id = "your_instance_id"
database_id = "your_database_id"
spanner_client = spanner_v1.Client(project=project_id)

# Create a Spanner database instance
instance = spanner_client.instance(instance_id)
pool = pool.FixedSizePool(size = 10, logging_enabled=True)
database = instance.database(pool=pool, database_id=database_id, close_inactive_transactions=True)

transaction_time = []


def calculatePercentile(latencies):
    # sort the latencies array
    latencies.sort()

    # calculate p50 (50th percentile)
    p50Index = math.floor(0.5*len(latencies))
    p50Latency = latencies[p50Index]

    # calculate p90 (90th percentile)
    p90Index = math.floor(0.9*len(latencies))
    p90Latency = latencies[p90Index]

    return [p50Latency, p90Latency]

# [START spanner_query_data]
def query_data(thread_id):
    print("running thread ", thread_id)
    start_time = time.time()
    time.sleep(10)
    with database.snapshot() as snapshot:
        results = snapshot.execute_sql(
            "SELECT 1 FROM Singers"
        )

    # for row in results:
    #     print(row)

    # for row in results:
    #     print("SingerId: {}, FirstName: {}, LastName: {}".format(*row))

    end_time = time.time()
    transaction_time.append(end_time-start_time)
# [END spanner_query_data]

# [START spanner_batch_transaction]
def batch_transaction(thread_id):
    print("running thread ", thread_id)
    start_time = time.time()
    time.sleep(10)
    batch_txn = database.batch_snapshot()
    batches = batch_txn.execute_sql(
        'SELECT * FROM Singers',
    )
    results = []
    for batch in batches:
        results.append("SingerId: {}, FirstName: {}, LastName: {}".format(*batch))

    # for batch in batches:
    #     for row in batch_txn.process_read_batch(batch):
    #         results.append("SingerId: {}, FirstName: {}, LastName: {}".format(*row))
    # for result in results:
    #     print(result)
   
    end_time = time.time()
    transaction_time.append(end_time-start_time)
# [END spanner_batch_transaction]

# [START insert_with_dml]
def insert_with_dml(i):
    """Inserts data with a DML statement into the database."""
    print("running thread ", i)
    start_time = time.time()
    time.sleep(10)

    def insert_singers(transaction):
        row_ct = transaction.execute_update(
            "INSERT Singers (SingerId, FirstName, LastName) VALUES ({}, 'Google{}', 'India{}')".format(i, i, i)
        )
        print("{} record(s) inserted.".format(row_ct))

    database.run_in_transaction(insert_singers)
    end_time = time.time()
    transaction_time.append(end_time-start_time)
# [END insert_with_dml]

# Define the number of threads
num_threads = 20
starting = 1

# Create and start the threads
threads = []
start = time.time()
for i in range(starting,starting+num_threads):
    thread = threading.Thread(target=query_data, args=(i,))
    thread.start()
    threads.append(thread)
    

# Wait for all threads to complete
for thread in threads:
    thread.join()

print("All threads have completed.")
end = time.time()

# Print the total execution time
print("total time taken by the execution: ", end-start)

#Writing transaction time to an output file
for t in transaction_time:
    with open ('output.txt', 'a') as file:  
        file.write(str(t)+"\n")
# latency = calculatePercentile(transaction_time)
# print("p50 latency is: ", latency[0])
# print("p90 latency is: ", latency[1])