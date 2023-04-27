import pymongo

class Mongosearch(object):
    def __init__(self):
        self.client = pymongo.MongoClient("mongodb://localhost:27017/")
        self.db = self.client["tweet_project"]
        self.col_ori = self.db["origin_tweet"]
        self.col_reply = self.db["reply_tweet"]
        self.col_quoted = self.db["quote_tweet"]
        self.col_retweet = self.db["retweet_tweet"]
        
    def search_all_col(self, tweet_id):
        query = { "tweet_id" : {"$eq" : None }}
        query['tweet_id']['$eq'] = tweet_id
        content = []
        if len(content) == 0:
            content += list(self.col_ori.find(query))
        
        if len(content) == 0:
            content += list(self.col_reply.find(query))
            
        if len(content) == 0:
            content += list(self.col_quoted.find(query))
        
        if len(content) == 0:
            content += list(self.col_retweet.find(query))
        return content  
        
    def keyword_search(self, searchString, sortMethod = "timestamp_ms", numofEntries = 100):
        query = {"$text": {"$search": None}}
        query['$text']['$search'] = searchString
        content = []
        if len(content) < numofEntries:
            content += list(self.col_ori.find(query).sort(sortMethod, 1).limit(numofEntries-len(content)))
        if len(content) < numofEntries:
            content += list(self.col_reply.find(query).sort(sortMethod, 1).limit(numofEntries-len(content)))
        if len(content) < numofEntries:
            content += list(self.col_quoted.find(query).sort(sortMethod, 1).limit(numofEntries-len(content)))
        if len(content) < numofEntries:
            content += list(self.col_retweet.find(query).sort(sortMethod, 1).limit(numofEntries-len(content)))
        return content
        
    
    def hashtag_search(self, searchString, sortMethod = "timestamp_ms", numofEntries = 100):
        query = {"hashtags.text": {"$eq": None}}
        query['hashtags.text']['$eq'] = searchString  
        content = []
        if len(content) < numofEntries:
            content += list(self.col_ori.find(query).sort(sortMethod, 1).limit(numofEntries-len(content)))
        if len(content) < numofEntries:
            content += list(self.col_reply.find(query).sort(sortMethod, 1).limit(numofEntries-len(content)))
        if len(content) < numofEntries:
            content += list(self.col_quoted.find(query).sort(sortMethod, 1).limit(numofEntries-len(content)))
        if len(content) < numofEntries:
            content += list(self.col_retweet.find(query).sort(sortMethod, 1).limit(numofEntries-len(content)))
        return content
    
    def complete_tweetid_search(self, tweet_id):
        doc = list(self.search_all_col(tweet_id))
        if len(doc) != 0:
            wait_list = [] + doc[0]["reply_list"]
        else:
            wait_list = []
        content = [] + doc
        while len(wait_list)!=0:
            reply_id = wait_list[0]
            wait_list.pop(0)

            reply_query = { "tweet_id" : {"$eq" : None }}
            reply_query['tweet_id']['$eq'] = reply_id

            doc = list(col_reply.find(reply_query))
            if "reply_list" in doc[0]:
                wait_list += doc[0]["reply_list"]
            content += doc
        return content
    
    def tweetid_search(self, tweet_id):
        doc = list(self.search_all_col(tweet_id))
        content = [] + doc
        return content
    
    def quote_list_search(self, tweet_id):
        doc = self.search_all_col(tweet_id)
        try:
            wait_list = doc[0]["quoted_list"]
        except:
            wait_list = []
        content = []
        for tid in wait_list:
            quote_query = { "tweet_id" : {"$eq" : None }}
            quote_query['tweet_id']['$eq'] = tid
            doc = list(self.col_quoted.find(quote_query))
            content += doc
        return content
      
    def retweet_list_search(self, tweet_id):
        doc = self.search_all_col(tweet_id)
        try:
            wait_list = doc[0]["retweeted_list"]
        except:
            wait_list = []
#         content = []
#         for tid in wait_list:
#             retweet_query = { "tweet_id" : {"$eq" : None }}
#             retweet_query['tweet_id']['$eq'] = tid
#             doc = list(self.col_retweet.find(retweet_query))
#             content += doc
        return wait_list