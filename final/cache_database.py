import pickle
import heapq
import time
import os

class cache_database(object):
    
    def __init__(self,file_path):
        self.max_item_saved = 30 # number of max searching result saved
        self.every_item_cleaned = 3 # number of cleaned item when max searching result arrived
        self.file_path = file_path
        self.list_file_path = self.file_path+"cache_database_list.pkl"
        self.dict_file_path = self.file_path+"cache_database_dict.pkl"
        try:
            with open(self.list_file_path, "rb") as f:
                self._queue = pickle.load(f)
            with open(self.dict_file_path, "rb") as f:
                self._dict = pickle.load(f)
        except:
            self._queue = list()
            self._dict = dict()
    
    def search(self, querry):
        (timestamp_list, querry, content) = self._dict[querry]
        return querry, content
                      
    def push(self, querry, content):
        """
        Smallest heap, node will be oldest querry
        Time complexity : O(logN)
        """
        timestamp_ms = str(int(time.time()*1000))
        heapq.heappush(self._queue, (timestamp_ms, querry, content))
        if querry in self._dict:
            timestamp_list = self._dict[querry][0] + [timestamp_ms,]
        else:
            timestamp_list = [timestamp_ms,]
        self._dict[querry] = (timestamp_list, querry, content)
        
        if self.qsize() >= self.max_item_saved:
            for i in range(self.every_item_cleaned):
                self.pop()
            with open(self.list_file_path, "wb") as f:
                pickle.dump(self._queue, f)
            with open(self.dict_file_path, "wb") as f:
                pickle.dump(self._dict, f)       
        else:
            with open(self.list_file_path, "wb") as f:
                pickle.dump(self._queue, f)
            with open(self.dict_file_path, "wb") as f:
                pickle.dump(self._dict, f)

    def pop(self):
        """
        Time complexity : O(logN)
        """
        timestamp_ms, querry, content = heapq.heappop(self._queue)
        self._dict[querry][0].pop(timestamp_ms)
        if len(self._dict[querry][0]) == 0:
            self._dict.pop(querry)
        return timestamp_ms, querry, content

    def qsize(self):
        return len(self._queue)

    def clean_memory(self):
        self._queue = list()
        self._dict = dict()
        if os.path.exists(self.list_file_path):
            os.remove(self.list_file_path) 
        if os.path.exists(self.dict_file_path):
            os.remove(self.dict_file_path)