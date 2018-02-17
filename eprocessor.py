import os
import json
import base64
import logging
import requests
import threading
import traceback
import cf_deployment_tracker

from flask import Flask

from pymongo import MongoClient
from pymongo import ASCENDING

from config import Config as config

from bots.OkeyApple import Process

# Emit Bluemix deployment event
cf_deployment_tracker.track()

app = Flask(__name__)

eprocessor_logger = logging.getLogger('eProcessor')


class processor(threading.Thread):
    def __init__(self):
        self.AUTO_MESSAGE = 0
        self.HUMAN_MESSAGE = 1
        self.HUMAN_AS_BOT = 3
        self.SILENCE = 4
        self.ECHO = 5  # Auto message sent and received back
        threading.Thread.__init__(self)

    def run(self):
        self.mongo_client, self.mongo_db = self.connect_db()
        while 1:
            try:
                self.raw_to_event()
            except Exception as e:
                eprocessor_logger.error(traceback.format_exc())

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
            if "is_echo" in message["message"] and message["message"]["is_echo"]:
                if "metadata" in message["message"] and message["message"]["metadata"] == "BOT_MESSAGE":
                    event_type = self.ECHO
            data_one["message"] = m
            data_one["from_id"] = message["sender"]["id"]
            data_one["to_id"] = message["recipient"]["id"]
            data_one["timestamp"] = int(message["timestamp"] / 1000)
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
            if change["value"]["verb"] != "add":
                continue
            if change["value"]["parent_id"] == change["value"]["post_id"]:
                comment_on_root = True
            else:
                comment_on_root = False
            if entry["id"] == change["value"]["from"]["id"]:  # Page ID == Sender ID
                event_type = self.AUTO_MESSAGE
            else:
                event_type = self.HUMAN_MESSAGE
            if event_type in [self.HUMAN_AS_BOT, self.AUTO_MESSAGE]:
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
            if comment_on_root:
                where_ = change["value"]["comment_id"]
            else:
                where_ = change["value"]["parent_id"]
            data_one["message"] = change["value"]["message"]
            data_one["from_id"] = change["value"]["from"]["id"]
            data_one["to_id"] = to_id
            data_one["timestamp"] = change["value"]["created_time"]
            data_one["where"] = where_
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
            # Find Bot asociated with from_id
            query = {"integrations.facebook.instagram_id": entry["id"]}
            bot = self.mongo_db["db_bots"]["bots"].find_one(query)
            if bot is None:
                continue

            # Find User
            access_token = bot["integrations"]["facebook"]["user_access_token"]
            if access_token is None:
                continue
            params = {
                "access_token": access_token
            }
            url = "https://graph.facebook.com/v2.11/{}?fields=user".format(change["value"]["id"])
            r = requests.get(url, params=params)
            r_json = r.json()
            if "error" in r_json:
                print(r.text)
                continue
            print(r.text)
            user_instagram_id = r_json["user"]["id"]

            if user_instagram_id == entry["id"]:
                event_type = self.AUTO_MESSAGE
            else:
                event_type = self.HUMAN_MESSAGE

            data_one["message"] = change["value"]["text"]
            data_one["from_id"] = user_instagram_id
            data_one["to_id"] = entry["id"]
            data_one["timestamp"] = entry["time"]
            data_one["where"] = change["value"]["id"]  # comment id
            data_one["type"] = event_type
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

    def get_user_data(self, page_access_token, user_id):
        params = {
            "access_token": page_access_token
        }
        url = "https://graph.facebook.com/v2.11/{}".format(user_id)
        r = requests.get(url, params=params)
        r_json = r.json()
        print(r.text)
        if "error" in r_json:
            print(r.text)
            return None
        return r_json

    def get_event_data(self, data):
        event = {
            "type": None,
            "conversation_id": None,
            "content": None,
            "timestamp": None,
            "source": None,
            "answer_to": None
        }
        user_dict = {
            "facebook_asid": None,
            "facebook_psid": None,
            "facebook_id": None,
            "bussiness_token": None,
            "instagram_id": None,
            "telegram_id": None,
            "data": {}
        }
        if data["source"] in ["feed", "messenger"]:
            if data["type"] in [self.HUMAN_AS_BOT, self.ECHO, self.AUTO_MESSAGE]:
                # Find Bot asociated with from_id (page)
                query = {"integrations.facebook.page_id": data["from_id"]}
                bot = self.mongo_db["db_bots"]["bots"].find_one(query)
                if bot is None:
                    print("Bot is None")
                    return None
                bot_mongo_id = bot["_id"]
                if data["source"] == "feed":
                    # Find User asociated with to_id
                    query = {"facebook_asid": data["to_id"]}
                    user_asid = self.mongo_db["db_users"]["users"].find_one(query)
                    user = user_asid
                elif data["source"] == "messenger":
                    # Find User asociated with page scoped id (psid)
                    query = {"facebook_psid": data["to_id"]}
                    user_psid = self.mongo_db["db_users"]["users"].find_one(query)
                    user = user_psid
                if user is None:
                    user_data = self.get_user_data(bot["integrations"]["facebook"]["page_access_token"], data["to_id"])
                    if data["source"] == "feed":
                        f_asid = data["to_id"]
                        f_psid = None
                        if user_data is not None:
                            user["data"]["name"] = {"!is": user_data["name"]}
                    elif data["source"] == "messenger":
                        f_asid = None
                        f_psid = data["to_id"]
                        if user_data is not None:
                            user_dict["data"]["first_name"] = {"!is": user_data["first_name"]}
                            user_dict["data"]["last_name"] = {"!is": user_data["last_name"]}
                            user_dict["data"]["gender"] = {"!is": user_data["gender"]}
                            user_dict["data"]["timezone"] = {"!is": user_data["timezone"]}
                    user_dict["facebook_asid"] = f_asid
                    user_dict["facebook_psid"] = f_psid
                    user_mongo_id = self.mongo_db["db_users"]["users"].insert_one(user_dict).inserted_id
                else:
                    user_mongo_id = user["_id"]
            elif data["type"] == self.HUMAN_MESSAGE:
                # Find Bot asociated with to_id (page)
                query = {"integrations.facebook.page_id": data["to_id"]}
                bot = self.mongo_db["db_bots"]["bots"].find_one(query)
                if bot is None:
                    print(query)
                    print("Bot is None B")
                    return None
                bot_mongo_id = bot["_id"]
                # Find User asociated with to_id
                if data["source"] == "feed":
                    query = {"facebook_asid": data["from_id"]}
                    user = self.mongo_db["db_users"]["users"].find_one(query)
                elif data["source"] == "messenger":
                    # Find User asociated with page scoped id (psid)
                    query = {"facebook_psid": data["from_id"]}
                    user = self.mongo_db["db_users"]["users"].find_one(query)
                if user is None:
                    user_data = self.get_user_data(bot["integrations"]["facebook"]["page_access_token"], data["from_id"])
                    if data["source"] == "feed":
                        f_asid = data["from_id"]
                        f_psid = None
                        if user_data is not None:
                            user_dict["data"]["name"] = {"!is": user_data["name"]}
                    elif data["source"] == "messenger":
                        f_asid = None
                        f_psid = data["from_id"]
                        if user_data is not None:
                            user_dict["data"]["first_name"] = {"!is": user_data["first_name"]}
                            user_dict["data"]["last_name"] = {"!is": user_data["last_name"]}
                            user_dict["data"]["gender"] = {"!is": user_data["gender"]}
                            user_dict["data"]["timezone"] = {"!is": user_data["timezone"]}
                    user_dict["facebook_asid"] = f_asid
                    user_dict["facebook_psid"] = f_psid
                    user_mongo_id = self.mongo_db["db_users"]["users"].insert_one(user_dict).inserted_id
                else:
                    user_mongo_id = user["_id"]
        elif data["source"] == "instagram":
            if data["type"] in [self.HUMAN_AS_BOT, self.ECHO, self.AUTO_MESSAGE]:
                # Find Bot asociated with from_id (instagram page)
                query = {"integrations.facebook.instagram_id": data["from_id"]}
                bot = self.mongo_db["db_bots"]["bots"].find_one(query)
                if bot is None:
                    print("Bot is None C")
                    return None
                bot_mongo_id = bot["_id"]
                # Find User asociated with to_id
                query = {"instagram_id": data["to_id"]}
                user = self.mongo_db["db_users"]["users"].find_one(query)
                if user is None:
                    user_access_token = bot["integrations"]["facebook"]["user_access_token"]
                    if user_access_token is None:
                        return None
                    params = {
                        "access_token": user_access_token
                    }
                    url = "https://graph.facebook.com/v2.11/{}?fields=name,username".format(data["to_id"])
                    r = requests.get(url, params=params)
                    user_data = r.json()
                    print(r.text)
                    if "error" in user_data:
                        print(r.text)
                        user_data = None
                    if user_data is not None:
                        user_dict["data"]["name"] = {"!is": user_data["name"]}
                        user_dict["data"]["username"] = {"!is": user_data["username"]}
                    user_dict["instagram_id"] = data["to_id"]
                    user_mongo_id = self.mongo_db["db_users"]["users"].insert_one(user_dict).inserted_id
                else:
                    user_mongo_id = user["_id"]
            elif data["type"] == self.HUMAN_MESSAGE:
                # Find Bot asociated with to_id (page)
                query = {"integrations.facebook.instagram_id": data["to_id"]}
                bot = self.mongo_db["db_bots"]["bots"].find_one(query)
                if bot is None:
                    print("Bot is None D")
                    return None
                bot_mongo_id = bot["_id"]
                # Find User asociated with to_id
                query = {"instagram_id": data["from_id"]}
                user = self.mongo_db["db_users"]["users"].find_one(query)
                if user is None:
                    user_access_token = bot["integrations"]["facebook"]["user_access_token"]
                    if user_access_token is None:
                        return None
                    params = {
                        "access_token": user_access_token
                    }
                    url = "https://graph.facebook.com/v2.11/{}?fields=name,username".format(data["from_id"])
                    r = requests.get(url, params=params)
                    user_data = r.json()
                    print(r.text)
                    if "error" in user_data:
                        print(r.text)
                        user_data = None
                    if user_data is not None:
                        user_dict["data"]["name"] = {"!is": user_data["name"]}
                        user_dict["data"]["username"] = {"!is": user_data["username"]}
                    user_dict["instagram_id"] = data["from_id"]
                    user_mongo_id = self.mongo_db["db_users"]["users"].insert_one(user_dict).inserted_id
                else:
                    user_mongo_id = user["_id"]
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
                'text': answer,
                'metadata': 'BOT_MESSAGE'
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

    def facebookPostComment(entry, comment_id, message, page_access_token):
        headers = {
            'user-agent': 'Eibriel Platform',
            'Content-Type': 'application/json'
        }
        params = {
            "access_token": page_access_token
        }
        commentData = {
            "message": message
        }
        url = "https://graph.facebook.com/v2.11/{}/comments".format(comment_id)
        r = requests.post(url, json=commentData, headers=headers, params=params)
        print(r.text)

    def answer(self, new_events):
        # Group by conversation
        # Sort inside each conversation
        # Process in order
        query = {"_id": {"$in": new_events}}
        events_mongo = self.mongo_db["db_events"]["events"].find(query).sort("timestamp", ASCENDING)
        conversations = {}
        for event_mongo in events_mongo:
            str_id = (str(event_mongo["conversation_id"]))
            if str_id not in conversations:
                conversations[str_id] = []
            conversations[str_id].append(event_mongo)
        for conversation in conversations:
            # Previous message type
            # Previous message timestamp
            query = {
                "conversation_id": conversations[conversation][0]["conversation_id"],
                "type": {"$ne": self.ECHO}
            }
            all_events_mongo = self.mongo_db["db_events"]["events"].find(query).sort("timestamp", ASCENDING)
            for event in conversations[conversation]:
                if event["type"] != self.HUMAN_MESSAGE:
                    continue
                previous_event = {
                    "AUTO_MESSAGE": None,
                    "HUMAN_AS_BOT": None,
                    "HUMAN_MESSAGE": None
                }
                for event_on_all in all_events_mongo:
                    if event_on_all["_id"] != event["_id"]:
                        if event_on_all["type"] == self.AUTO_MESSAGE:
                            previous_event["AUTO_MESSAGE"] = event_on_all
                        elif event_on_all["type"] == self.HUMAN_AS_BOT:
                            previous_event["HUMAN_AS_BOT"] = event_on_all
                        elif event_on_all["type"] == self.HUMAN_MESSAGE:
                            previous_event["HUMAN_MESSAGE"] = event_on_all
                    else:
                        break
                previous_data = {
                    "AUTO_MESSAGE": {"timestamp": None
                                     },
                    "HUMAN_AS_BOT": {"timestamp": None
                                     },
                    "HUMAN_MESSAGE": {"timestamp": None
                                      }
                }
                if previous_event["AUTO_MESSAGE"] is not None:
                    previous_data["AUTO_MESSAGE"]["timestamp"] = previous_event["AUTO_MESSAGE"]["timestamp"]
                if previous_event["HUMAN_AS_BOT"] is not None:
                    previous_data["HUMAN_AS_BOT"]["timestamp"] = previous_event["HUMAN_AS_BOT"]["timestamp"]
                if previous_event["HUMAN_MESSAGE"] is not None:
                    previous_data["HUMAN_MESSAGE"]["timestamp"] = previous_event["HUMAN_MESSAGE"]["timestamp"]

                pr = Process()
                if event["source"] == "messenger":
                    query = {"facebook_psid": event["content"]["from_id"]}
                elif event["source"] == "feed":
                    query = {"facebook_asid": event["content"]["from_id"]}
                elif event["source"] == "instagram":
                    query = {"instagram_id": event["content"]["from_id"]}
                else:
                    query = None
                user_data = None
                if query is not None:
                    user = self.mongo_db["db_users"]["users"].find_one(query)
                    if user is not None:
                        user_data = user["data"]
                answer = pr.get_answer(event["content"]["message"], previous_data, user_data)
                if answer is None:
                    return
                from_id = event["content"]["to_id"]
                to_id = event["content"]["from_id"]
                # Find Bot asociated with from_id
                if event["source"] == "instagram":
                    query = {"integrations.facebook.instagram_id": from_id}
                    bot = self.mongo_db["db_bots"]["bots"].find_one(query)
                    if bot is not None:
                        user_access_token = bot["integrations"]["facebook"]["user_access_token"]
                else:
                    query = {"integrations.facebook.page_id": from_id}
                    bot = self.mongo_db["db_bots"]["bots"].find_one(query)
                    if bot is not None:
                        page_access_token = bot["integrations"]["facebook"]["page_access_token"]

                if event["source"] == "messenger":
                    self.facebookSendTextMessage(from_id, to_id, answer, page_access_token)
                elif event["source"] == "feed":
                    if "Gracias por ponerte en contacto." not in answer:
                        comment_id = event["content"]["where"]
                        self.facebookPostComment(comment_id, answer, page_access_token)
                elif event["source"] == "instagram":
                    comment_id = event["content"]["where"]
                    headers = {
                        'user-agent': 'Eibriel Platform',
                        'Content-Type': 'application/json'
                    }
                    params = {
                        "access_token": user_access_token
                    }
                    params["message"] = "{} -+-".format(answer)
                    url = "https://graph.facebook.com/v2.11/{}/replies".format(comment_id)
                    r = requests.post(url, headers=headers, params=params)
                    # facebook_logger.error(r.text)

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
