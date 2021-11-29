FROM python:3.8-buster

RUN pip install tweepy && \
    pip install pandas && \
    pip install s3fs && \
    pip install pyarrow
