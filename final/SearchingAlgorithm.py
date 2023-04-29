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

    def quote_tweet_id(self, quote_id):
        myquery_quote = {"tweet_id": {"$eq": None}}
        myquery_quote['tweet_id']['$eq'] = quote_id
        mydoc_quote = self.quote_tweet.find(myquery_quote)
        result = list(mydoc_quote)[0]
        return result

    def retweet_tweet_id(self, retweet_id):
        myquery_retweet = {"tweet_id": {"$eq": None}}
        myquery_retweet['tweet_id']['$eq'] = retweet_id
        mydoc_retweet = self.retweet_tweet.find(myquery_retweet)
        result = list(mydoc_retweet)[0]
        return result

    def reply_tweet_id(self, reply_id):
        myquery_reply = {"tweet_id": {"$eq": None}}
        myquery_reply['tweet_id']['$eq'] = reply_id
        mydoc_reply = self.reply_tweet.find(myquery_reply)
        result = list(mydoc_reply)[0]
        return result

    def recursive_reply(self, key, reply_rec_list):
        m = self.reply_tweet_id(key)
        reply_rec_list.append(m)
        myquery_rec_reply = {"tweet_id": {"$eq": None}}
        myquery_rec_reply['tweet_id']['$eq'] = key
        mydoc_rec_reply = list(self.reply_tweet.find(myquery_rec_reply))
        for j_len in range(len(mydoc_rec_reply)):
            if "reply_list" in mydoc_rec_reply[j_len].keys():
                temp = mydoc_rec_reply[j_len]["reply_list"]
                for each_reply in temp:
                    self.recursive_reply(each_reply, reply_rec_list)
        return reply_rec_list

    def keyword_search_in_reply(self, search_str, num_origin_tweets, sort_in = "dsc"):
        criteria = "keyword"
        sort_method = "timestamp_ms"
        if sort_in == "desc":
            sort_by = -1
        else:
            sort_by = 1
        if criteria == "keyword":
            myquery = {"$text": {"$search": None}}
            myquery['$text']['$search'] = search_str
            mydoc_replies = self.reply_tweet.find(myquery).sort(sort_method, sort_by).limit(num_origin_tweets)
        n = []

        for each_tweet in list(mydoc_replies):
            n.append(each_tweet)
        return n

    def top_10(self):
        most_famous = self.original_tweet.find().sort("custom_score", -1).limit(10)
        return list(most_famous)

    def search_algorithm(self, search_string, criteria, sort_method='timestamp_ms', num_origin_tweets=10,
                         num_replies=10, num_quotes=10, num_retweets=10, min_res_count=10, sort_in = dsc):
        ori_tweet = []
        quoted_rec_list = []
        retweeted_rec_list = []
        reply_rec_list = []
        keyword_replies = []
        if sort_in == "desc":
            sort_by = -1
        else:
            sort_by = 1
        if criteria in ["keyword", "hashtags", "tweet_id"]:
            if criteria == "keyword":
                myquery = {"$text": {"$search": None}}
                myquery['$text']['$search'] = search_string
                if sort_method == "default":
                    mydoc = self.original_tweet.find(myquery, {'score': {'$meta': 'textScore'}}).sort(
                        [('score', {'$meta': 'textScore'})]).limit(num_origin_tweets)
                else:
                    mydoc = self.original_tweet.find(myquery).sort(sort_method, sort_by).limit(num_origin_tweets)
            if criteria == "hashtags":
                myquery = {"hashtags.text": {"$eq": None}}
                myquery['hashtags.text']['$eq'] = search_string
                mydoc = self.original_tweet.find(myquery).sort(sort_method, sort_by).limit(num_origin_tweets)
            if criteria == "tweet_id":
                myquery = {"tweet_id": {"$eq": None}}
                myquery['tweet_id']['$eq'] = search_string
                mydoc = self.original_tweet.find(myquery).sort(sort_method, sort_by).limit(num_origin_tweets)
        list_of_results_original = list(mydoc)

        for each_tweet in list_of_results_original:
            ori_tweet.append(each_tweet)
            try:
                for each_reply in range(min(len(each_tweet["reply_list"]), num_replies)):
                    reply_rec_list = self.recursive_reply(each_tweet["reply_list"][each_reply], [])
            except:
                pass

            try:
                for each_quote in range(min(len(each_tweet["quoted_list"]), num_quotes)):
                    quoted_rec_list.append(self.quote_tweet_id(each_tweet["quoted_list"][each_quote]))
            except:
                pass

            try:
                for each_retweet in range(min(len(each_tweet["retweeted_list"]), num_retweets)):
                    retweeted_rec_list.append(self.retweet_tweet_id(each_tweet["retweeted_list"][each_retweet]))
            except:
                pass

        if len(list_of_results_original) < min_res_count:
            num = 10 - len(list_of_results_original)
            keyword_replies = self.keyword_search_in_reply(search_string, num_origin_tweets=num)

        return ori_tweet, reply_rec_list, quoted_rec_list, retweeted_rec_list, keyword_replies

# n = SearchMong()
# n.search_algorithm("quran", "keyword")
# n.top_10()
# n.search_algorithm("1253927939713966086","tweet_id", "timestamp_ms", 1)
# n.search_algorithm("quran", "keyword", sort_method = "default")
# n.search_algorithm("quran", "keyword", sort_method = "default", sort_in = "asc")

# n.search_algorithm("coffee","hashtags")

# n.search_algorithm("covid","keyword", num_origin_tweets = 1)
