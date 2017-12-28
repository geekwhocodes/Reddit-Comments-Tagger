from threading import Thread
import pyodbc
from ult import isAbusive, cleanText
import badwords
import sys

class TaggerWorker(Thread):
    '''
        Tagger worker
    '''

    def __init__(self, queue, conn):
        Thread.__init__(self)
        self.queue = queue
        self._n = 0
        self.__connection_string = conn
        self.__bad_words = badwords.get()

    def get_connection(self):
        con = pyodbc.connect(self.__connection_string, autocommit=True, timeout=120)
        return con

    def run(self):
        while self._n <= 1592920:
            # Get the work from the queue
            try:
                rowid, body = self.queue.get()
                if rowid % 10 is 0:
                    print("Proccessing Task with rowid : {0} and body : {1} ".format(rowid, body)) 
                
                # check if comment id abusive
                sent = cleanText(body)
                w = set(sent.lower().strip().split(' '))
                b = set(self.__bad_words)
                result = w.intersection(b)
                if len(result) > 0:
                    q = "update Comments set isAbusive = 1, isProc = 1 where id = " + str(rowid)
                    with self.get_connection() as conn:
                        cursor = conn.cursor()
                        cursor.execute(q)
                        self.queue.task_done()
                else:
                    q = "update Comments set isProc = 1 where id = " + str(rowid)
                    with self.get_connection() as conn:
                        cursor = conn.cursor()
                        cursor.execute(q)
                        self.queue.task_done()
                self._n +=1
            except:
                print ("Unexpected error:", sys.exc_info()[0])
