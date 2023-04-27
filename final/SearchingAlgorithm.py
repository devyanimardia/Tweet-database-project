import pymongo


class SearchMong(object):
    def __init__(self):
        self.mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
        self.mongo_db = self.mongo_client["tweet_project"]
        self.original_tweet = self.mongo_db["origin_tweet"]
        self.reply_tweet = self.mongo_db["reply_tweet"]
        self.retweet_tweet = self.mongo_db["retweet_tweet"]
        self.quote_tweet = self.mongo_db["quote_tweet"]

    def create_indexes(self):
        self.original_tweet.create_index([('text', 'text'), ('hashtags', 'text'), ('tweet_id', -1)])
        self.reply_tweet.create_index([('text', 'text'), ('hashtags', 'text'), ('tweet_id', -1)])
        self.retweet_tweet.create_index([('text', 'text'), ('hashtags', 'text'), ('tweet_id', -1)])
        self.quote_tweet.create_index([('text', 'text'), ('hashtags', 'text'), ('tweet_id', -1)])

    def custom_rank(self):
        """This function is to get a customized weighted average rating to sort our tweets as per their popularity"""
        self.original_tweet.update_many(
            {},
            [
                {"$set": {
                    "custom_score": {"$add": ["$favorite_count", "$reply_count", "$retweet_count", "$quote_count"]}}}
            ]
        )

    def quote_tweet(self, quote_id):
        myquery_quote = {"tweet_id": {"$eq": None}}
        myquery_quote['tweet_id']['$eq'] = quote_id
        mydoc_quote = self.quote_tweet.find(myquery_quote)
        sam = list(mydoc_quote)[0]
        return [sam["tweet_id"], sam["text"], sam["username"], sam["favorite_count"]]

    def retweet_tweet(self, retweet_id):
        myquery_retweet = {"tweet_id": {"$eq": None}}
        myquery_retweet['tweet_id']['$eq'] = retweet_id
        mydoc_retweet = self.retweet_tweet.find(myquery_retweet)
        sam = list(mydoc_retweet)[0]
        return [sam["tweet_id"], sam["text"], sam["username"], sam["favorite_count"]]

    def reply_tweet(self, reply_id):
        myquery_reply = {"tweet_id": {"$eq": None}}
        myquery_reply['tweet_id']['$eq'] = reply_id
        mydoc_reply = self.reply_tweet.find(myquery_reply)
        result = list(mydoc_reply)[0]
        return [result["tweet_id"], result["text"], result["username"], result["favorite_count"]]

    def recursive_reply(self, key, reply_rec_list):
        reply_rec_list.append(self.reply_tweet(key))
        myquery_rec_reply = {"tweet_id": {"$eq": None}}
        myquery_rec_reply['tweet_id']['$eq'] = key
        mydoc_rec_reply = list(self.reply_tweet.find(myquery_rec_reply))
        for j_len in range(len(mydoc_rec_reply)):
            if "reply_list" in mydoc_rec_reply[j_len].keys():
                temp = mydoc_rec_reply[j_len]["reply_list"]
                for each_reply in temp:
                    self.recursive_reply(each_reply, reply_rec_list)
        return reply_rec_list

    def keyword_search_in_reply(self, search_str, num_origin_tweets):
        criteria = "keyword"
        sort_method = "timestamp_ms"
        if criteria == "keyword":
            myquery = {"$text": {"$search": None}}
            myquery['$text']['$search'] = search_str
            mydoc_replies = self.reply_tweet.find(myquery).sort(sort_method, -1).limit(num_origin_tweets)
        n = []
        print("\n Search in reply yields: \n")
        for each_tweet in list(mydoc_replies):
            n.append(each_tweet)
        return n

    def top_10(self):
        print("Random top tweets")
        most_famous = self.original_tweet.find().sort("custom_score", -1).limit(10)
        return list(most_famous)

    def search_algorithm(self, search_string, criteria, sort_method="timestamp_ms", num_origin_tweets=10,
                         num_replies=10, num_quotes=10, num_retweets=10, min_res_count=10):
        ori_tweet = []
        quoted_rec_list = []
        retweeted_rec_list = []
        reply_rec_list = []

        if criteria in ["keyword", "hashtags", "tweet_id"]:
            if criteria == "keyword":
                myquery = {"$text": {"$search": None}}
                myquery['$text']['$search'] = search_string
                mydoc = self.original_tweet.find(myquery, {'score': {'$meta': 'textScore'}}).sort(
                    [('score', {'$meta': 'textScore'})]).limit(num_origin_tweets)
            if criteria == "hashtags":
                myquery = {"hashtags.text": {"$eq": None}}
                myquery['hashtags.text']['$eq'] = search_string
                mydoc = self.original_tweet.find(myquery).sort(sort_method, -1).limit(num_origin_tweets)
            if criteria == "tweet_id":
                myquery = {"tweet_id": {"$eq": None}}
                myquery['tweet_id']['$eq'] = search_string
                mydoc = self.original_tweet.find(myquery).sort(sort_method, -1).limit(num_origin_tweets)
        list_of_results_original = list(mydoc)
        
        for each_tweet in list_of_results_original:
            ori_tweet.append(each_tweet)
            try:
                for each_reply in range(min(len(each_tweet["reply_list"]), num_replies)):
                    reply_rec_list = self.recursive_reply(each_tweet["reply_list"][each_reply], reply_rec_list=[])
                    [print(i) for i in reply_rec_list]
            except:
                print("No further replies needed")

            try:
                for each_quote in range(min(len(each_tweet["quoted_list"]), num_quotes)):
                    quoted_rec_list.append(self.quote_tweet(each_tweet["quoted_list"][each_quote]))
            except:
                print("No further quoted tweets needed")

            try:
                for each_retweet in range(min(len(each_tweet["retweeted_list"]), num_retweets)):
                    retweeted_rec_list.append(self.retweet_tweet(each_tweet["retweeted_list"][each_retweet]))
            except:
                print("No further retweets needed")

        if len(list_of_results_original) < min_res_count:
            num = 10 - len(list_of_results_original)
            self.keyword_search_in_reply(search_string, num_origin_tweets=num)

        return ori_tweet, reply_rec_list, quoted_rec_list, retweeted_rec_list

