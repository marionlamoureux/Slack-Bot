#!pip install boto3
#!pip install slack
#!pip install flask
#!pip install slackeventsapi

import csv
import json
import os
import re
import uuid

import slack_sdk.web
import boto3

from datetime import datetime
from pathlib import Path

from flask import Flask, request, Response
from slackeventsapi import SlackEventAdapter


app = Flask(__name__)
slack_event_adapter = SlackEventAdapter(
  os.environ["SIGNING_SECRET_"],
  "/slack/events", app)

BUCKET = "tothemoon-dl"
FOLDER = "my-data/moon-landing/slack"

s3 = boto3.resource(
    service_name='s3',
    region_name='us-east-1',
    aws_access_key_id=os.environ['S3_ACCESS_KEY'],
    aws_secret_access_key=os.environ["S3_SECRET_KEY"]
)


# We will use this to check each message that comes in.
meme_list = [
  "BBBY",
  "AMC",
  "GME",
  "TSLA",
  "NOKIA",
  "BB",
  "PLTR",
  "SPCE",
  "Bitcoin"]
meme_patt_string = "(" + "|".join(meme_list) + ")"
meme_patt = re.compile(meme_patt_string)

@slack_event_adapter.on("message")
def message(payload):
    event = payload.get("event",{})

    # We're not interested in "message_deleted" events
    subtype = event.get("subtype")
    if subtype and subtype == "message_deleted":
        return
    
    msg_text = event.get("text")
    
    # Does the message mention a meme stock?
    mentioned_meme_stocks = meme_patt.findall(msg_text)
    if not mentioned_meme_stocks:
        # We didn't find any meme stocks, but make sure list
        # contains one element, for iteration immediately below:
        mentioned_meme_stocks = [None]
    
    # Generate output files
    for meme in mentioned_meme_stocks:
        # Give each message file a unique, content-based name
        timestamp = datetime.utcfromtimestamp(float(event.get("event_ts"))).isoformat()
        unique_id = str(uuid.uuid4())
        filename = f"{timestamp.replace(':','-')}__{unique_id}.csv"
      
        with open(filename, "a") as f:
            writer = csv.writer(f, quoting=csv.QUOTE_ALL)
            writer.writerow([
                "timestamp",
                "messageid",
                "channel",
                "author",
                "text",
                "kind",
                "symbol"])
            writer.writerow([
                timestamp,
                event.get("client_msg_id"),
                event.get("channel"),
                event.get("user"),
                msg_text,
                "slack",
                meme])

        # Upload to S3
        s3.Bucket(BUCKET).upload_file(
            Key=str(Path(FOLDER, filename)),  # Path() takes care of clean concatenation
            Filename=filename)

        # Tidy up
        os.remove(filename)
    
    # Curious to see what a payload looks like
    # with open("payloads.txt", "a") as f:
    #     f.write(json.dumps(payload) + "\n")


if __name__ == "__main__":
    app.run(host='127.0.0.1', port=int(os.environ["CDSW_APP_PORT"]))