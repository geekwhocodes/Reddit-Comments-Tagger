import pandas as pd
import sqlite3
from queue import Queue
from threading import Thread
from time import time

from TaggerWorker import TaggerWorker
from CommentTagger import CommentTagger

# connection = sqlite3.connect("../input/database.sqlite")
# #q = "SELECT subreddit, body, score FROM May2015 WHERE score > 25  LIMIT 200"
# q = "SELECT COUNT(*) FROM May2015 WHERE score > 25 "

def main():
    ''' Run Comment Tagger with proper config.abs
    '''
    t = time()
    connection_string = "DRIVER={ODBC Driver 13 for SQL Server};SERVER=your.sql.server;DATABASE=your.data.name;UID=username;PWD=password"
    tagger = CommentTagger(100, connection_string, 25, 10, 'Comments')
    tagger.run()
    print("Time took :", time() - t)

if __name__ == '__main__':

    main()
    