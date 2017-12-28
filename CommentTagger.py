import pandas as pd
import pyodbc

from queue import Queue

from TaggerWorker import TaggerWorker
import ult

class CommentTagger():

    def __init__(self, batch_size=100, conn_string=None, score=25, num_jobs=1, table_name=''):
        if conn_string is None:
            raise Exception("please provide db connection credentials.")
        if table_name is None:
            raise Exception("Please provide valid table name")
        
        self.__connection_string = conn_string
        self.__batch_size = batch_size
        #self.set_connection()
        self.__table_name = table_name
        self.__total_num_rows = self.get_total_num_rows()
        self.__score = score
        self.__top = batch_size
        self.__skip = 0
        self.__jobs = num_jobs
        self.__queue = Queue()
        

        print("Tagger initiated...")

    def get_connection(self):
        con = pyodbc.connect(self.__connection_string, autocommit=True, timeout=120)
        return con

    def get_total_num_rows(self):
        q = "SELECT COUNT(*) FROM " + self.__table_name
        with self.get_connection() as conn:
            cursor = conn.cursor()
            c = cursor.execute(q)
            result = c.fetchall()
            return result[0][0]

    def get_batch_data(self):
        q = "SELECT id, body FROM " + self.__table_name + " WHERE isProc = 0 ORDER BY id OFFSET " + \
            str(self.__skip) + " ROWS FETCH NEXT " + str(self.__top) + " ROWS ONLY"
        with self.get_connection() as conn:
            data = pd.read_sql(q, conn)
            self.__top = self.__batch_size
            self.__skip = self.__skip + self.__top
            return data

    def create_start_workers(self):
        for w in range(0, self.__jobs):
            worker = TaggerWorker(self.__queue, self.__connection_string)
            worker.daemon = True
            worker.start()

    def put_on_queue(self, data):
        self.__queue.put((data.id, data.body))

    def run(self):
        '''
        get batch data from db 
        '''
        self.create_start_workers()

        for batch in range(0, int(self.__total_num_rows/self.__batch_size)):
            print("Proccessing batch no : {0}".format(batch))
            data = self.get_batch_data()
            data.apply(self.put_on_queue, axis=1)
        
        self.__queue.join()
