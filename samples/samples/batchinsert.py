
from google.cloud import spanner
import _thread
import pdb
import logging
logging.getLogger().addHandler(logging.FileHandler('logsSurbhi.log'))
logging.getLogger().setLevel(logging.DEBUG)
def batch_write(prefix):
    client = spanner.Client()
    instance = client.instance('surbhi-testing')
    database = instance.database('test-db')
    for i in range(1):
        values = [(prefix+str(i * 1000 + j).zfill(6), 'FN'+prefix+str(
            i * 1000 + j).zfill(6), 'LN'+prefix+str(
                i * 1000 + j).zfill(6)) for j in range(1000)]
        with database.batch() as batch:
            batch.insert_or_update(
                table='SingersABC',
                columns=('SingerId','FirstName','LastName'),
                values=values
            )
        print(u'Inserted batch {}'.format(prefix+str(i)))

if __name__ == "__main__":
    try:
        for prefix in 'abcdefghijklmnopqrstuvwxyz':
            _thread.start_new_thread(batch_write,(prefix,))
            print('started Thread' + prefix)
    except:
        print('Unable to start new thread')
    while 1:
        pass