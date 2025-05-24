from google.cloud import spanner
from google.cloud.spanner_v1 import RequestOptions

client = spanner.Client()
instance = client.instance('test-instance')
database = instance.database('test-db')

with database.snapshot() as snapshot:
    results = snapshot.execute_sql("SELECT * FROM all_types LIMIT 10")
    for row in results:
        print(row)

# database.drop()