import getopt
import os
import sys
from datetime import date

from S3TweetDfIO import S3TweetDfIO
from TwitterConnector import TwitterConnector


def parse_input_argv(argv) -> (str, str, str, int, str, str):
    search_word = None
    language = None
    result_type = None
    max_tweet_count = None
    bucket = None
    path = None
    hint = "TwitterBot.py -s <search_word> -l <language> -r <result_type> -m <max_tweet_count> -b <bucket_name> -o <output_path>" \
           "\n The allowed values for result_type are {mixed,recent, popular} " \
           "\nIf -r, -m is missing, it will use default value in config file"
    try:
        opts, args = getopt.getopt(argv, "hs:l:r:m:b:o", ["search_word=", "help", "language=", "result_tye=",
                                                          "max_tweet_count=", "bucket_name=", "output_path="])
    except getopt.GetoptError:
        raise SystemExit(f"invalide arguments \nhint: {hint}")
    for opt, arg in opts:
        # option h for help
        if opt in ('-h', "--help"):
            print("hint")
            sys.exit()
        # option for config file
        elif opt in ("-s", "--search_word"):
            search_word = arg
        elif opt in ("-l", "--language"):
            language = arg
        elif opt in ("-r", "--result_type"):
            result_type = arg
        elif opt in ("-m", "--max_tweet_count"):
            max_tweet_count = arg
        elif opt in ("-b", "--bucket_name"):
            bucket = arg
        elif opt in ("-o", "--output_path"):
            path = arg
        else:
            raise SystemExit(f"unknown option.\n hint:{hint}")
    if (not opts) or (len(args) > 1) or (len(opts) > 6):
        raise SystemExit(f"invalide arguments \nhint: {hint}")
    return search_word, language, result_type, max_tweet_count, bucket, path


def main(argv):
    # Configure TwitterConnector with twitter developer account creds
    consumer_key = os.getenv("TWITTER_CONSUMER_KEY")
    consumer_secret = os.getenv("TWITTER_CONSUMER_SECRET")
    access_token = os.getenv("TWITTER_ACCESS_TOKEN")
    access_token_secret = os.getenv("TWITTER_ACCESS_TOKEN_SECRET")
    if consumer_key is None or consumer_secret is None or access_token is None or access_token_secret is None:
        raise SystemExit(f"Incomplete arguments: Twitter credential in env var is incomplete ")
    # create an instance of twitter connector
    tc = TwitterConnector(consumer_key, consumer_secret, access_token, access_token_secret)
    # filter the search result by using below key words
    search_word, language, result_type, max_tweet_count, bucket, output_path = parse_input_argv(argv)

    # get the tweets
    tweets = tc.get_tweets(search_word, language, result_type, max_tweet_count)
    # generate a pandas df from the tweet
    df = tc.generate_tweet_df(tweets)

    # s3 creds config
    endpoint = os.getenv("AWS_S3_ENDPOINT")
    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    access_secret = os.getenv("AWS_SECRET_ACCESS_KEY")
    token = os.getenv("AWS_SESSION_TOKEN")
    # create an instance of the tweet io
    if endpoint is None or access_key is None or access_secret is None or token is None:
        raise SystemExit(f"Incomplete arguments: S3 credential in env var is incomplete ")

    s3_tweet_io = S3TweetDfIO(endpoint, access_key, access_secret, token)
    print(f"{endpoint}+{access_key}+{access_secret}+{token}")
    # set up write path
    current_date = date.today().strftime("%d-%m-%Y")
    f_path = f"{output_path}/tweet_{current_date}"
    s3_tweet_io.write_df_to_s3(df, bucket, output_path)

    # set up read example
    df_read = s3_tweet_io.read_parquet_from_s3(bucket, f_path)
    print(df_read.head())


if __name__ == "__main__":
    main(sys.argv[1:])
