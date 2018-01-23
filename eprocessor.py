import os
import json
import base64
import requests
import threading
import cf_deployment_tracker

from flask import Flask

from pymongo import MongoClient
from pymongo import ASCENDING

from config import Config as config

# Emit Bluemix deployment event
cf_deployment_tracker.track()

app = Flask(__name__)


class processor(threading.Thread):
    def __init__(self):
        self.AUTO_MESSAGE = 0
        self.HUMAN_MESSAGE = 1
        self.HUMAN_AS_BOT = 3
        self.SILENCE = 4
        threading.Thread.__init__(self)

    def run(self):
        self.mongo_client, self.mongo_db = self.connect_db()
        while 1:
            self.raw_to_event()

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
                "db_bots": client["{}bots".format(prefix)],
                "db_users": client["{}users".format(prefix)],
                "db_raw_input": client["{}raw_input".format(prefix)],
                "db_events": client["{}events".format(prefix)],
                "db_conversations": client["{}conversations".format(prefix)]
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
                "where": None,
                "type": None,
                "source": "messenger"
            }
            needs_answer = False
            if "message" in message:
                senderId = message['sender']['id']
                if "text" in message["message"]:
                    m = message["message"]["text"]
                elif "sticker_id" in message["message"]:
                    m = "sticker_id {}".format(message["message"]["sticker_id"])
            elif "read" in message:
                senderId = message['sender']['id']
                m = "[read]"
            elif "postback" in message:
                if message["postback"].get("payload") == "Get Started":
                    senderId = message['sender']['id']
                    m = None
            if message['sender']['id'] == entry["id"]:
                event_type = self.HUMAN_AS_BOT
            else:
                event_type = self.HUMAN_MESSAGE
            data_one["message"] = m
            data_one["from_id"] = message["sender"]["id"]
            data_one["to_id"] = message["recipient"]["id"]
            data_one["timestamp"] = message["timestamp"]
            data_one["type"] = event_type
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
                "where": None,
                "type": None,
                "source": "feed"
            }
            if change["value"]["item"] != "comment":
                continue
            if change["value"]["parent_id"] == change["value"]["post_id"]:
                comment_on_root = True
            else:
                comment_on_root = False
            if entry["id"] == change["value"]["from"]["id"]:  # Page ID == Sender ID
                event_type = self.HUMAN_AS_BOT
            else:
                event_type = self.HUMAN_MESSAGE
            if event_type == self.HUMAN_AS_BOT:
                if comment_on_root:
                    # We don't know who is talking to
                    continue
                else:
                    # Who is talking to?
                    # We only know the parent_id
                    # Find Bot asociated with from_id
                    query = {"integrations.facebook.page_id": entry["id"]}
                    bot = self.mongo_db["db_bots"]["bots"].find_one(query)
                    if bot is None:
                        continue
                    access_token = bot["integrations"]["facebook"]["page_access_token"]
                    if access_token is None:
                        continue
                    params = {
                        "access_token": access_token
                    }
                    url = "https://graph.facebook.com/v2.11/{}".format(change["value"]["parent_id"])
                    r = requests.get(url, params=params)
                    r_json = r.json()
                    if "error" in r_json:
                        print(r.text)
                        continue
                    print(r.text)
                    to_id = r_json["from"]["id"]
            else:  # Human message
                to_id = entry["id"]

            data_one["message"] = change["value"]["message"]
            data_one["from_id"] = change["value"]["from"]["id"]
            data_one["to_id"] = to_id
            data_one["timestamp"] = change["value"]["created_time"]
            data_one["where"] = change["value"]["comment_id"]
            data_one["type"] = event_type
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
                "where": None,
                "type": None,
                "source": "instagram"
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
        if raw["source"] == "facebook":
            if "object" in raw["data"] and raw["data"]['object'] == 'page':
                for entry in raw["data"]['entry']:
                    if "messaging" in entry:
                        data = self.process_facebook_messenger(entry)
                    elif "changes" in entry:
                        data = self.process_facebook_feed(entry)
            elif "object" in raw["data"] and raw["data"]['object'] == 'instagram':
                for entry in raw["data"]['entry']:
                    if "changes" in entry:
                        data = self.process_instagram_comment(entry)
        return data

    def get_event_data(self, data):
        event = {
            "type": None,
            "conversation_id": None,
            "content": None,
            "timestamp": None,
            "source": None,
            "answer_to": None
        }
        if data["source"] in ["feed", "messenger"]:
            if data["type"] == self.HUMAN_AS_BOT:
                # Find Bot asociated with from_id
                query = {"integrations.facebook.page_id": data["from_id"]}
                bot = self.mongo_db["db_bots"]["bots"].find_one(query)
                if bot is None:
                    print("Bot is None")
                    return None
                bot_mongo_id = bot["_id"]
                # Find User asociated with to_id
                query = {"facebook_id": data["to_id"]}
                user = self.mongo_db["db_users"]["users"].find_one(query)
                if user is None:
                    user = {
                        "facebook_id": data["to_id"],
                        "instagram_id": None,
                        "telegram_id": None,
                        "data": {}
                    }
                    user_mongo_id = self.mongo_db["db_users"]["users"].insert_one(user).inserted_id
                else:
                    user_mongo_id = user["_id"]
            else:
                # Find Bot asociated with from_id
                query = {"integrations.facebook.page_id": data["to_id"]}
                bot = self.mongo_db["db_bots"]["bots"].find_one(query)
                if bot is None:
                    print("Bot is None B")
                    return None
                bot_mongo_id = bot["_id"]
                # Find User asociated with to_id
                query = {"facebook_id": data["from_id"]}
                user = self.mongo_db["db_users"]["users"].find_one(query)
                if user is None:
                    user = {
                        "facebook_id": data["from_id"],
                        "instagram_id": None,
                        "telegram_id": None,
                        "data": {}
                    }
                    user_mongo_id = self.mongo_db["db_users"]["users"].insert_one(user).inserted_id
                else:
                    user_mongo_id = user["_id"]
        elif data["source"] == "instagram":
            pass
        # Find Conversation asociated with user and bot
        query = {
            "bot_id": bot_mongo_id,
            "user_id": user_mongo_id,
            "platform": data["source"]
        }
        print(query)
        conversation = self.mongo_db["db_conversations"]["conversations"].find_one(query)
        if not conversation:
            conversation = {
                "bot_id": bot_mongo_id,
                "user_id": user_mongo_id,
                "platform": data["source"]
            }
            conversation_mongo_id = self.mongo_db["db_conversations"]["conversations"].insert_one(conversation).inserted_id
        else:
            conversation_mongo_id = conversation["_id"]

        event["type"] = data["type"]
        event["conversation_id"] = conversation_mongo_id
        event["content"] = data
        event["timestamp"] = data["timestamp"]
        event["source"] = data["source"]

        return event

    def facebookSendTextMessage(self, from_id, to_id, answer, access_token):
        messageData = {
            'recipient': {
                'id': to_id
            },
            'message': {
                'text': answer
            },
            'messaging_type': 'RESPONSE'
        }
        self.callSendAPI(messageData, from_id, access_token)

    def callSendAPI(self, messageData, page_id, page_access_token, endpoint="messages"):
        # facebook_logger.info(messageData)
        print(messageData, page_id, page_access_token, endpoint)
        headers = {'user-agent': 'Eibriel Platform',
                   'Content-Type': 'application/json'}
        # page_access_token = app.config["FB_PAGE_ACCESS_TOKEN"]
        # page_access_token = get_page_token(app.config["FB_PAGE_ID"])
        # page_id = app.config["FB_PAGE_ID"]
        r = None
        while 1:
            try:
                r = requests.post('https://graph.facebook.com/v2.11/{}/{}?access_token={}'.format(page_id, endpoint, page_access_token), json=messageData, headers=headers)
                break
            except requests.exceptions.ConnectionError:
                raise
                if retry:
                    print("Facebook: ConnectionError")
                else:
                    break
            except requests.exceptions.Timeout:
                raise
                if retry:
                    print("Facebook: Timeout")
                else:
                    break
            time.sleep(5)
        print(r.text)
        # facebook_logger.error(r.text)
        return r

    def answer(self, new_events):
        # Group by conversation
        # Sort inside each conversation
        # Process in order
        query = {"_id": {"$in": new_events}}
        events_mongo = self.mongo_db["db_events"]["events"].find(query).sort("timestamp", ASCENDING)
        conversations = {}
        for event_mongo in events_mongo:
            str_id = (str(event_mongo["_id"]))
            if str_id not in conversations:
                conversations[str_id] = []
            conversations[str_id].append(event_mongo)
        for conversation in conversations:
            for event in conversations[conversation]:
                if event["type"] != self.HUMAN_MESSAGE:
                    continue
                answer = event["content"]["message"]
                if event["source"] == "messenger":
                    from_id = event["content"]["to_id"]
                    to_id = event["content"]["from_id"]
                    # Find Bot asociated with from_id
                    query = {"integrations.facebook.page_id": from_id}
                    bot = self.mongo_db["db_bots"]["bots"].find_one(query)
                    if bot is not None:
                        access_token = bot["integrations"]["facebook"]["page_access_token"]
                        self.facebookSendTextMessage(from_id, to_id, answer, access_token)

    def raw_to_event(self):
        query = {"status": 0}
        update = {"$set": {"status": 1}}
        raw = self.mongo_db["db_raw_input"]["raw_input"].find_one_and_update(query, update)
        # print(raw)
        if raw is None:
            return
        data = self.process_raw_data(raw)
        if data is None:
            query = {"_id": raw["_id"]}
            update = {"$set": {"status": -1}}
            self.mongo_db["db_raw_input"]["raw_input"].update_one(query, update)
            return
        new_events = []
        for data_one in data:
            event = self.get_event_data(data_one)
            print(event)
            if event is not None:
                event_mongo_id = self.mongo_db["db_events"]["events"].insert_one(event).inserted_id
                new_events.append(event_mongo_id)
        query = {"_id": raw["_id"]}
        update = {"$set": {"status": 2}}
        self.mongo_db["db_raw_input"]["raw_input"].update_one(query, update)
        self.answer(new_events)


@app.route('/ping')
def ping():
    return "pong"

processor_thread = processor()
processor_thread.start()

port = int(os.getenv('PORT', 5000))

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=port, debug=True)
