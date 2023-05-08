from google.cloud import spanner
from google.cloud.spanner import TransactionPingingPool
from google.cloud.spanner_v1 import param_types

pool = TransactionPingingPool(size=30, default_timeout=5, ping_interval=300)
client = spanner.Client()
instance = client.instance('surbhi-testing')
database = instance.database('test-db', pool=pool)

def unit_of_Work(transaction, id):
        
        # transaction.update(
        #     table="Singers",
        #     columns=("SingerId", "FirstName"),
        #     values=[(id, "Surbhi1")],
        # )
        # row_ct = transaction.execute_update(
        #     "INSERT Singers (SingerId, FirstName, LastName) "
        #     " VALUES ({id}, 'Virginia {id}', 'Watson {id}')".format(id=id)
        # )
        # print("{} record(s) inserted.".format(row_ct))

        results = transaction.execute_sql(
            "SELECT FirstName, LastName FROM Singers WHERE SingerId = {id}".format(id=id)
        )

        for result in results:
            print("FirstName: {}, LastName: {}".format(*result))
        
        row_ct = transaction.execute_update(
            "UPDATE Singers SET LastName = 'Grant' "
            "WHERE SingerId = {id}".format(id=id)
        )
        print("{} record(s) updated.".format(row_ct))

        results = transaction.execute_sql(
            "SELECT FirstName, LastName FROM Singers WHERE SingerId = {id}".format(id=id)
        )

        for result in results:
            print("FirstName: {}, LastName: {}".format(*result))

def execute_transaction():
    database.run_in_transaction(unit_of_Work, id=20)
# singerid = 17

# while singerid < 67:
#     database.run_in_transaction(unit_of_Work, id=singerid)
#     singerid = singerid +1
import timeit
num_runs = 10
num_repetions = 3
ex_time = timeit.Timer(execute_transaction).repeat(repeat=num_repetions, number = num_runs)
print(f'It took {ex_time}')

