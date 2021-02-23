import requests
import pandas as pd
import datetime


def create_url(query, fields, expansions, since_id=None, next_token=None):
    # Tweet fields are adjustable.
    # Options include:
    # attachments, author_id, context_annotations,
    # conversation_id, created_at, entities, geo, id,
    # in_reply_to_user_id, lang, non_public_metrics, organic_metrics,
    # possibly_sensitive, promoted_metrics, public_metrics, referenced_tweets,
    # source, text, and withheld
    tweet_fields = "tweet.fields={}".format(fields)
    if next_token:
        if since_id:
            url = "https://api.twitter.com/2/tweets/search/recent?query={}&{}&{}&{}&{}".format(query, tweet_fields,
                                                                                               expansions, since_id,
                                                                                               next_token)
        else:
            url = "https://api.twitter.com/2/tweets/search/recent?query={}&{}&{}&{}".format(query, tweet_fields,
                                                                                            expansions, next_token)
    else:
        if since_id:
            url = "https://api.twitter.com/2/tweets/search/recent?query={}&{}&{}&{}".format(query, tweet_fields,
                                                                                            expansions, since_id)
        else:
            url = "https://api.twitter.com/2/tweets/search/recent?query={}&{}&{}".format(query, tweet_fields,
                                                                                         expansions)

    return url


def combine_tweets_users(t, u):
    for i in t:
        for j in u:
            if i['author_id'] == j['id']:
                i['name'] = j['name']
                i['username'] = j['username']
    return t


def get_type(json_arr):
    json_el = json_arr[0]
    tp = json_el['type']
    return tp


def get_ref_id(json_arr):
    json_el = json_arr[0]
    id = json_el['id']
    return id


def newest_tweet(new_r, new_r_t, new_q, new_q_t):
    if new_r:
        # if both
        if new_q:
            if new_r_t > new_q_t:
                newest = new_r
            else:
                newest = new_q
        # if just r
        else:
            newest = new_r

    # if just  q
    elif new_q:
        newest = new_q

    else:
        newest = None

    return newest


def first_order_response(tweet_id: str, fields: str, expansions, headers, since_id=None):
    reply_query = "conversation_id:{}".format(tweet_id)
    quote_rt_query = "url:{}".format(tweet_id)
    expansions = "expansions={}".format(expansions)
    if since_id:
        since = "since_id={}".format(since_id)

    reply_url = create_url(reply_query, fields, expansions)
    quote_rt_url = create_url(quote_rt_query, fields, expansions)

    if since_id:
        reply_url = create_url(reply_query, fields, expansions, since)
        quote_rt_url = create_url(quote_rt_query, fields, expansions, since)

    replies, newest_r, newest_r_time = get_response(reply_query, reply_url, fields, expansions, headers)
    quotes_rts, newest_q, newest_q_time = get_response(quote_rt_query, quote_rt_url, fields, expansions, headers)

    newest = newest_tweet(newest_r, newest_r_time, newest_q, newest_q_time)

    return replies, quotes_rts, newest  # combined replies and quote_rts


def filter_first_order(tweet: dict, query: str):
    is_first_order = False;

    if 'referenced_tweets' in tweet:

        # Filter first order replies
        if tweet['referenced_tweets'][0]['type'] == 'replied_to':
            id = query.replace("conversation_id:", "")

            # If the reply id is equal to the root tweet id
            if tweet['referenced_tweets'][0]['id'] == id:
                is_first_order = True

        # Filter first order quotes
        if len(tweet['referenced_tweets']) == 1:
            if tweet['referenced_tweets'][0]['type'] == 'quoted':
                id = query.replace("url:", "")

                # If the reply id is equal to the root tweet id
                if tweet['referenced_tweets'][0]['id'] == id:
                    is_first_order = True

    return is_first_order


def get_response(query: str, url: str, fields: str, expansions, headers):
    keepSearching = 1
    tweets = []
    users = []
    while (keepSearching == 1):

        # get request
        response = requests.request("GET", url, headers=headers)
        conversation = response.json()

        if 'data' in conversation:
            # newest tweet id
            newest = conversation['meta']['newest_id']

            for i in conversation['data']:
                # get time of newest id
                if i["id"] == newest:
                    newest_time = i["created_at"]

                if filter_first_order(i, query):
                    tweets.append(i)

            for i in conversation['includes']['users']:
                users.append(i)

            # see if more pages
            if 'next_token' not in conversation['meta']:
                keepSearching = 0

            else:
                next_token = "next_token=" + conversation['meta']['next_token']
                url = create_url(query, fields, expansions, next_token)

        # data is not in conversation
        else:
            keepSearching = 0
            newest = None
            newest_time = None

    # combine tweets and users
    results = combine_tweets_users(tweets, users)

    return results, newest, newest_time


# create labeled dataframe
def create_df(replies, qts):
    new_column_names = ["tweet_id", "tweet_datetime", "tweet_text", "author_id", "author_name", "author_username",
                        "tweet_conversation_id", "tweet_language", "tweet_source", "reference_type",
                        "referenced_tweet_id"]

    column_names = ["id", "created_at", "text", "author_id", "name", "username", "referenced_tweets", "conversation_id",
                    "lang", "source"]

    if not replies:
        rDf = pd.DataFrame(columns=column_names)
    else:
        rDf = pd.DataFrame.from_dict(replies)[column_names]

    if not qts:
        qDf = pd.DataFrame(columns=column_names)
    else:
        qDf = pd.DataFrame.from_dict(qts)[column_names]

    df = pd.concat([rDf, qDf], ignore_index=True)

    df['reference_type'] = df.referenced_tweets.apply(get_type)
    df['referenced_tweet_id'] = df.referenced_tweets.apply(get_ref_id)
    df.drop('referenced_tweets', axis=1, inplace=True)

    df.columns = new_column_names
    return df

