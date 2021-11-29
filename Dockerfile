FROM python:3.8-buster

COPY data_ingestion /bots
COPY requirements.txt /bots
RUN pip3 install -r /bots/requirements.txt

WORKDIR /bots
CMD ["python3", "TwitterBot.py"]