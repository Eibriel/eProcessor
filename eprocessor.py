import os
import json
import base64
import threading
import cf_deployment_tracker

from flask import Flask

from pymongo import MongoClient

from config import Config as config

# Emit Bluemix deployment event
cf_deployment_tracker.track()

app = Flask(__name__)


class processor(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        mongo_client, mongo_db = self.connect_db()
        while 1:
            self.raw_to_event(mongo_db)

    def connect_db(self):
        prefix = "dev_"
        if 'VCAP_SERVICES' in os.environ:
            prefix = ""
            vcap = json.loads(os.getenv('VCAP_SERVICES'))
            print('Found VCAP_SERVICES')
        elif "LOCAL_ENV" in config:
            vcap = config["LOCAL_ENV"]
            print('Found local VCAP_SERVICES')
        else:
            print('No Cloudant')
            return None, None
        if 'compose-for-mongodb' in vcap:
            creds = vcap['compose-for-mongodb'][0]['credentials']
            uri = creds["uri"]
        try:
            ca_certs = base64.b64decode(creds["ca_certificate_base64"])
            with open('cert', 'w') as f:
                f.write(ca_certs.decode("utf-8"))
            client = MongoClient(uri, ssl=True, ssl_ca_certs="cert")
        except:
            raise
            print("Cloudant Error")
        try:
            db = {
                "db_raw_input": client["{}raw_input".format(prefix)],
                "db_events": client["{}events".format(prefix)]
            }
        except:
            raise
            print("Cloudant Error")
        return client, db

    def process_facebook_messenger(self, entry):
        data = []
        for message in entry['messaging']:
            data_one = {
                "message": None,
                "from_id": None,
                "to_id": None,
                "timestamp": None,
                "where": None
            }
            needs_answer = False
            if "message" in message:
                senderId = message['sender']['id']
                if "text" in message["message"]:
                    m = message["message"]["text"]
                    needs_answer = True
                elif "sticker_id" in message["message"]:
                    m = "sticker_id {}".format(message["message"]["sticker_id"])
                    needs_answer = True
            elif "read" in message:
                senderId = message['sender']['id']
                m = "[read]"
                # needs_answer = True
            elif "postback" in message:
                if message["postback"].get("payload") == "Get Started":
                    senderId = message['sender']['id']
                    m = None
                    needs_answer = True
            if needs_answer:
                data_one["message"] = m
                data_one["from_id"] = entry["sender"]["id"]
                data_one["to_id"] = entry["recipient"]["id"]
                data_one["timestamp"] = entry["timestamp"]
                data.append(data_one)
        return data

    def process_facebook_feed(self, entry):
        data = []
        for change in entry["changes"]:
            data_one = {
                "message": None,
                "from_id": None,
                "to_id": None,
                "timestamp": None,
                "where": None
            }
            if change["value"]["item"] != "comment":
                continue
            data_one["message"] = change["value"]["message"]
            data_one["from_id"] = change["from"]["id"]
            data_one["to_id"] = entry["id"]
            data_one["timestamp"] = change["value"]["created_time"]
            data_one["where"] = change["comment_id"]
            data.append(data_one)
        return data

    def process_instagram_comment(self, entry):
        data = []
        for change in entry["changes"]:
            data_one = {
                "message": None,
                "from_id": None,
                "to_id": None,
                "timestamp": None,
                "where": None
            }
            if change["field"] != "comments":
                continue
            comment_id = change["value"]["id"]
            message = change["value"]["text"]
            data_one["message"] = message
            data_one["from_id"] = change["value"]["id"]
            data_one["to_id"] = entry["id"]
            data_one["timestamp"] = change["time"]
            data_one["where"] = comment_id
            data.append(data_one)
        return data

    def process_raw_data(self, raw):
        data = None
        if "object" in raw and raw['object'] == 'page':
            for entry in raw['entry']:
                if "messaging" in entry:
                    data = self.process_facebook_messenger(entry)
                elif "changes" in entry:
                    data = self.process_facebook_feed(entry)
        elif "object" in raw and raw['object'] == 'instagram':
            for entry in raw['entry']:
                if "changes" in entry:
                    data = self.process_instagram_comment(entry)
        return data

    def raw_to_event(self, db):
        event = {
            "type": None,
            "conversation_id": None,
            "content": None,
            "timestamp": None,
            "source": None,
            "answer_to": None
        }
        query = {"status": 0}
        update = {"$set": {"status": 1}}
        raw = db["db_raw_input"]["raw_input"].find_one_and_update(query, update)
        # print(raw)
        if raw is None:
            return
        data = self.process_raw_data(raw["data"])
        if data is None:
            query = {"_id": raw["_id"]}
            update = {"$set": {"status": -1}}
            db["db_raw_input"]["raw_input"].update_one(query, update)
        for data_one in data:
            event["content"] = data_one["message"]
            db["db_events"]["events"].insert_one(event)
        query = {"_id": raw["_id"]}
        update = {"$set": {"status": 2}}
        db["db_raw_input"]["raw_input"].update_one(query, update)


@app.route('/ping')
def ping():
    return "pong"

processor_thread = processor()
processor_thread.start()
# processor_thread.join()

port = int(os.getenv('PORT', 5000))

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=port, debug=True)
