from cache_database import cache_database
from Mongosearch import Mongosearch

class tweet_search(object):
    def __init__(self):
        cache_file_path = ""
        self.cache_db = cache_database(cache_file_path)
        self.ms = Mongosearch()
        
    def cache_search(self, in_querry):
        out_querry,out_content = self.cache_db.search(in_querry)
        return out_content
    
    def keyword_search(self, in_querry):
        try:
            output = self.cache_search(in_querry)
            self.cache_db.push(in_querry,output)
            return output
        except KeyError as e:
            output = self.ms.keyword_search(in_querry)
            self.cache_db.push(in_querry,output)
            return output
    
    def hashtag_search(self, in_querry):
        try:
            output = self.cache_search(in_querry)
            self.cache_db.push(in_querry,output)
            return output
        except KeyError as e:
            output = self.ms.hashtag_search(in_querry)
            self.cache_db.push(in_querry,output)
            return output
    
    def user_search(self, in_querry):
        try:
            output = self.cache_search(in_querry)
            self.cache_db.push(in_querry,output)
            return output
        except KeyError as e:
            pass # TODO use user search function
            pass # TODO use use simple tweetid search to get all tweet of the user
    
    def tweetid_search(self, in_querry):
        try:
            output = self.cache_search(in_querry)
            self.cache_db.push(in_querry,output)
            return output
        except KeyError as e:
            output = self.ms.complete_tweetid_search(in_querry)
            self.cache_db.push(in_querry,output)
            return output
        
    def quote_list_search(self, in_querry):
        try:
            output = self.cache_search(in_querry)
            self.cache_db.push(in_querry,output)
            return output
        except KeyError as e:
            output = self.ms.quote_list_search(in_querry)
            self.cache_db.push(in_querry,output)
            return output
        
    def retweet_list_search(self, in_querry):
        try:
            output = self.cache_search(in_querry)
            self.cache_db.push(in_querry,output)
            return output
        except KeyError as e:
            output = self.ms.retweet_list_search(in_querry)
            self.cache_db.push(in_querry,output)
            return output