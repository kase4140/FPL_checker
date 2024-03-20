import json
from kafka import KafkaProducer
import time
import os
import logging
import requests
import pandas

def get_data():
    # this will have all first 3 queries
    res = requests.get("https://fantasy.premierleague.com/api/leagues-classic/1084104/standings/")
    res = res.json()
    second_json = []
    for i in res["standings"]["results"]:
        res2 = requests.get("https://fantasy.premierleague.com/api/entry/"+str(i["entry"]))
        res2 = res2.json()
        second_json.append(res2)

    return res, second_json

def transformation(res, res2):
    return {"ranking":res, "players":res2}

def streaming():
    curr_time = time.time()
    producer = KafkaProducer(bootstrap_servers=["kafka1:19092","kafka2:19093","kafka3:19094"])

    while True:
        if time.time() > curr_time + 120:
            break
        try:
            res, res2 = get_data()
            data = transformation(res, res2)
            print(json.dumps(data, indent=4))
            producer.send("fpl_topic", json.dumps(data).encode("utf-8"))
            time.sleep(2)
        except Exception as e:
            logging.error(f"an error accurred: {e}")
            continue
    return

    # print(res2["player_first_name"])
    # for i in res2["leagues"]["classic"]:
    #     print(i["name"])
    # print()
    # print(res2["player_region_name"]+" "+res2["player_first_name"] +" "+res2["leagues"]["classic"][0]["name"])
    # if res2["leagues"]["classic"][0]["name"] == "Canada":
    #     print(None)
    # else:
    #     print(res2["leagues"]["classic"][0]["name"])
