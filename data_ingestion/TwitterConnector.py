import logging
import tweepy as tw
import pandas as pd


class TwitterConnector:
    """
    A class to represent a twitter client connection.

    ...

    Attributes
    ----------
    client_api : Tweepy.api.API
        A tweet client api that can get tweets.

    Methods
    -------
    _create_api(consumer_key, consumer_secret, access_token, access_token_secret):
         Create twitter client api

    get_tweets(search_words, lang, result_type, max_tweet_count):
        get tweets by using given condition

    generate_tweet_df(tweets):
       Transform raw tweets to a pandas data frame
    """

    def __init__(self, consumer_key: str, consumer_secret: str, access_token: str, access_token_secret: str):
        """Constructs an instance of tweeter client api

         Parameters
         ----------
             consumer_key : str
                 consumer_key of your twitter developer account
             consumer_secret : str
                 consumer_secret of your twitter developer account
             access_token : str
                 access_token of your twitter developer account
            access_token_secret: str
                 access_token_secret of twitter developer account
         """
        self.client_api = self._create_api(consumer_key, consumer_secret, access_token, access_token_secret)

    @staticmethod
    def _create_api(consumer_key: str, consumer_secret: str, access_token: str, access_token_secret: str):
        """ Create twitter client api

        This private function takes a twitter developer account credentials and create an instance of a tweeter
        client api

        Parameters
        ----------
             consumer_key : str
                 consumer_key of your twitter developer account
             consumer_secret : str
                 consumer_secret of your twitter developer account
             access_token : str
                 access_token of your twitter developer account
             access_token_secret: str
                 access_token_secret of twitter developer account

        Returns
        -------
              api : Tweepy.api.API
                  an instance of a tweeter client api
          """
        client_auth = tw.OAuthHandler(consumer_key, consumer_secret)
        client_auth.set_access_token(access_token, access_token_secret)
        api = tw.API(client_auth, wait_on_rate_limit=True, retry_count=5, retry_delay=1)
        try:
            api.verify_credentials()
        except Exception as e:
            logging.error("Error during authentication")
            raise e
        logging.info("Authentication OK")
        return api

    def get_tweets(self, search_words: str, lang: str, result_type: str, max_tweet_count: int):
        """ Get tweets that matche the search condition

        This function takes some search conditions and return an array of tweets that match these conditions

        Parameters
        ----------
             search_words : str
                 consumer_key of your twitter developer account
             lang : str
                 consumer_secret of your twitter developer account
             result_type : str
                 access_token of your twitter developer account
             max_tweet_count: int
                 access_token_secret of twitter developer account

        Returns
        -------
              tweets : tweepy.models.SearchResults
                  an array of tweets that matche the search condition
          """
        tweets = self.client_api.search_tweets(q=search_words, lang=lang, result_type=result_type, count=max_tweet_count)
        return tweets

    @staticmethod
    def generate_tweet_df(tweets):
        """ Transform raw tweets to a pandas data frame

        This function takes an array of tweets then transform them into a data frame

        Parameters
        ----------
             tweets : tweepy.models.SearchResults
                 an array of raw tweets

        Returns
        -------
            df : pandas.DataFrame
               A pandas dataframe that has the required fields of tweets
          """
        # init dataframe
        df = pd.DataFrame(columns=['name', 'date', 'text'])
        index = 0
        for tweet in tweets:
            # get column value for each tweet
            tweet_dict = tweet._json
            # add new row to the dataframe
            df.loc[index] = pd.Series({'name': tweet_dict.get("user").get("name"), 'date': tweet_dict.get("created_at"),
                                       'text': tweet_dict.get("text")})
            index = index + 1
        return df


def main():
    consumer_key = "changeMe"
    consumer_secret = "changeMe"
    access_token = "changeMe"
    access_token_secret = "changeMe"
    # create an instance of twitter connector
    tc = TwitterConnector(consumer_key, consumer_secret, access_token, access_token_secret)
    # filter the search result by using below key words
    search_words = "#insee"
    # We can get tweet before certain date
    # until_date = "2021-11-24
    # specify the
    language = "fr"
    # the max tweet number will be retained in the result
    max_tweet_count = 1000000
    result_type = "mixed"
    # get the tweets
    tweets = tc.get_tweets(search_words, language, result_type, max_tweet_count)
    # generate a pandas df from the tweet
    df = tc.generate_tweet_df(tweets)


if __name__ == "__main__":
    main()
