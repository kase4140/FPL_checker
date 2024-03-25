from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import json
from kafka import KafkaProducer
import time
import os
import logging
import requests
from kafka import KafkaConsumer
import pandas as pd
import json
from snowflake.connector.pandas_tools import write_pandas
from snowflake.connector import connect

default_args = {
    'owner': 'Hamdan',
    'start_date': days_ago(0),
    'email': ['hamdan@somemail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

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
    # curr_time = time.time()
    producer = KafkaProducer(bootstrap_servers=["kafka1:19092","kafka2:19093","kafka3:19094"])

    # while True:
    #     if time.time() > curr_time + 120:
    #         break
    #     try:
    res, res2 = get_data()
    data = transformation(res, res2)
    print(json.dumps(data, indent=4))
    producer.send("fpl_topic", json.dumps(data).encode("utf-8"))
    # time.sleep(2)
        # except Exception as e:
        #     logging.error(f"an error accurred: {e}")
        #     continue
    return
def get_data_kafka():
    # print("i got to the getting part from kafka: ")
    consumer = KafkaConsumer("fpl_topic",bootstrap_servers=["kafka1:19092","kafka2:19093","kafka3:19094"], auto_offset_reset='earliest',value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    return consumer

def creating_json():
    message = None
    # print("i got to the printing part: ")
    consumer = get_data_kafka()
    # df = pd.DataFrame()
    for message in consumer:
        message = json.loads(json.dumps(message.value))
        # json2 = pd.json_normalize(message)
        print("Received data: ", message)
        break
    return message

def dataframe_creations():

    json_kafka = creating_json()
    rankings = pd.json_normalize(json_kafka["ranking"]["standings"]["results"])
    players = pd.json_normalize(json_kafka["players"])
    # rank and the player name data frame
    df_player_rank = rankings[["player_name","rank"]]

    # the event total of the week and the players name
    df_player_event_total = rankings[["player_name","event_total"]]
    
    # player name and the total points collected
    df_player_total = rankings[["player_name","total"]]

    # player first name and the region he is from
    df_player_region = players[["player_first_name","player_region_name"]]

    # fav teams
    teams = []
    names = players[["player_first_name"]]
    jsons = json_kafka["players"]

    # in here i get the favourite team of each player and in the second part I grab each players leagues they are in:
    id_players = players[["id"]]
    names_players_leagues = players[["player_first_name"]]
    league_final =[]
    for i in jsons:
        # this part is where i get the fav team of each player
        fav_team = i["leagues"]["classic"][0]["name"]
        if fav_team == "Canada":
            teams.append("None")
        else:
            teams.append(fav_team)
        # in here is where i get the leagues of each player they are in
        league = []
        leagues = i["leagues"]["classic"]
        for j in leagues:
            league.append(j["name"])
        league_final.append(league)
        print()

    # fav teams data frame is being created:
    df_fav_team_names = pd.DataFrame()
    df_fav_team_names['names']=names
    df_fav_team_names["Favourite_Teams"] = teams


    # in here the first 2 for loops i put the data frame of player_first_name and id into a list, then in the last nested for loop is where i duplicate the names and the id so they can match each players league, so for example if one player has 5 league and the names and id will match it and duplicate it to become 5 names and ids
    names_list = []
    players_id = []
    for j,i in names_players_leagues.iterrows():
        names_list.append(i['player_first_name'])
    
    for j,i in id_players.iterrows():
        players_id.append(i['id'])
    final_leagues_names = []
    final_leagues_id = []
    j = 0
    for i in league_final:
        for k in i:
            final_leagues_names.append(names_list[j])
            final_leagues_id.append(players_id[j])
        j+=1
    # this is where i flatten the list so we can add it to the dataframe 
    league_final_flatten = [j for sub in league_final for j in sub]

    #  this is where the data frame of id, names and the league names of each player is being created
    leagues_df = pd.DataFrame()
    leagues_df["id"] = final_leagues_id
    leagues_df["names"] = final_leagues_names
    leagues_df["league_names"] = league_final_flatten

    return df_player_rank, df_player_event_total, df_player_total, df_player_region, df_fav_team_names, leagues_df

def sending_to_snowflake():
    conn = connect(
        user="hamdankasem",
        # just need to add the password
        password="",
        # account ID organizationID-AccountID
        account="",
        warehouse='FPL',
        database='fpl_data',
        schema='FPL',
    )
    print(conn)
    
    df_player_rank, df_player_event_total, df_player_total, df_player_region, df_fav_team_names, leagues_df = dataframe_creations()

    success1, _, _, _ = write_pandas(conn, df_player_rank, "player_rank", auto_create_table=True)
    success2, _, _, _ = write_pandas(conn, df_player_event_total, "player_event_total", auto_create_table=True)
    success3, _, _, _ = write_pandas(conn, df_player_total, "player_total", auto_create_table=True)
    success4, _, _, _ = write_pandas(conn, df_player_region, "player_region", auto_create_table=True)
    success5, _, _, _ = write_pandas(conn, df_fav_team_names, "player_fav_team", auto_create_table=True)
    success6, _, _, _ = write_pandas(conn, leagues_df, "player_leagues", auto_create_table=True)
    if success1 == True:
        print("table 1 is created")
    else:
        print("something went wrong for table 1")

    if success2 == True:
        print("table 2 is created")
    else:
        print("something went wrong for table 2")

    if success3 == True:
        print("table 3 is created")
    else:
        print("something went wrong for table 3")

    if success4 == True:
        print("table 4 is created")
    else:
        print("something went wrong for table 4")

    if success5 == True:
        print("table 5 is created")
    else:
        print("something went wrong for table 5")

    if success6 == True:
        print("table 6 is created")
    else:
        print("something went wrong for table 6")


    conn.close()

    return

def verify():
    conn = connect(
        user="hamdankasem",
        # just need to add the password
        password="",
        # account ID organizationID-AccountID
        account="",
        warehouse='FPL',
        database='fpl_data',
        schema='FPL',
    )
    cur = conn.cursor()
    sql = "SELECT * FROM FPL_DATA.FPL.\"player_event_total\""
    cur.execute(sql)
    data = cur.fetchall()
    df = pd.DataFrame(data, columns=[x[0] for x in cur.description])
    print(df)

    cur.close()
    conn.close()

    return 

with DAG("fpl_dag",
    default_args=default_args,
    description='this dag streams fpl data to kafka',
    schedule_interval='0 19 8 * 1'
) as dag:
    streaming_task = PythonOperator(
        task_id = 'streaming_data',
        python_callable=streaming,
    )

    sending_to_datawarehouse = PythonOperator(
        task_id = 'sending_data_snowflake',
        python_callable=sending_to_snowflake,
    )

    verify_data_snowflake = PythonOperator(
        task_id = 'verify_data_snowflake',
        python_callable=verify,
    )

    streaming_task >> sending_to_datawarehouse >> verify_data_snowflake