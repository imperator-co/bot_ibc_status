import discord
import json
import asyncio
import os
import glob
import ast
import requests
import traceback, time
from humanfriendly import format_timespan
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String

SCHEDULE_SECONDES = 60 # reduction by query time
url = "https://api-osmosis.imperator.co/ibc/v1/raw"

token_bot = #os.environ['BOT_DISCORD_TOKEN']
client = discord.Client()

channel_id_ibc_gang = #int(os.environ['BOT_DISCORD_CHANNEL_IBC_GANG'])

#Db declaration
engine = create_engine('sqlite:///database.db', echo = True)
meta = MetaData()

#IBC Status Table
ibc_status = Table(
   'ibc_status', meta, 
   Column('ibc_route', String, primary_key = True), 
   Column('last_tx', String), 
   Column('counter', String),
   Column('size_queue', String),
   Column('is_trigger', String),
)

def select_ibc_routes():
    print(f"Start SELECT * IBC_STATUS")
    all_routes = []
    conn = engine.connect()
    ibc_status_table_select_query = ibc_status.select()
    routes = conn.execute(ibc_status_table_select_query)
    for route in routes:
        print(f"'{route[0]}', '{route[1]}', '{route[2]}', '{route[3]}', '{route[4]}'")
        ibc_route = route[0]
        last_tx = route[1]
        counter = route[2]
        size_queue = route[3]
        is_trigger = route[4]
        path_info = {}
        path_info[ibc_route] = {"last_tx": last_tx, "counter": counter, "size_queue": size_queue, "is_trigger": is_trigger}
        all_routes.append(path_info)
    conn.close()
    return all_routes

def select_specific_ibc_routes(ibc_route):
    print(f"Start SELECT * IBC_STATUS WHERE IBC_ROUTE = {ibc_route}")
    path_info = {}
    conn = engine.connect()
    ibc_status_table_select_query = ibc_status.select().where(ibc_status.c.ibc_route==ibc_route)
    routes = conn.execute(ibc_status_table_select_query)
    for route in routes:
        print(f"'{route[0]}', '{route[1]}', '{route[2]}', '{route[3]}', '{route[4]}'")
        ibc_route = route[0]
        last_tx = route[1]
        counter = route[2]
        size_queue = route[3]
        is_trigger = route[4]
        path_info[ibc_route] = {"last_tx": last_tx, "counter": counter, "size_queue": size_queue, "is_trigger": is_trigger}
    conn.close()
    return path_info

def check_ibc_routes(ibc_route):
    print(f"Start SELECT * IBC_STATUS WHERE IBC_ROUTE = {ibc_route}")
    conn = engine.connect()
    ibc_status_table_select_query = ibc_status.select().where(ibc_status.c.ibc_route==ibc_route)
    routes = conn.execute(ibc_status_table_select_query)
    print(routes)
    if routes.first() is None:
        return False
    conn.close()
    return True

def insert_ibc_routes(ibc_route, last_tx, counter, size_queue, is_trigger):
    print(f"Start inserting {ibc_route}")
    print(ibc_route)
    print(last_tx)
    print(counter)
    print(size_queue)
    print(is_trigger)
    conn = engine.connect()
    ibc_status_table_insert_query = ibc_status.insert().values({"ibc_route": ibc_route, "last_tx": last_tx, "counter": counter, "size_queue": size_queue, "is_trigger": is_trigger})
    conn.execute(ibc_status_table_insert_query)
    conn.close()
    print(f"{ibc_route} inserted !")

def update_ibc_routes(ibc_route, last_tx, counter, size_queue, is_trigger):
    print(f"Start updating {ibc_route}")
    conn = engine.connect()
    ibc_status_table_update_query = ibc_status.update().where(ibc_status.c.ibc_route==ibc_route).values(last_tx=last_tx, counter=counter, size_queue=size_queue, is_trigger=is_trigger)
    conn.execute(ibc_status_table_update_query)
    conn.close()
    print(f"{ibc_route} updated !")

def reinit_path_relay(ibc_route, last_tx, counter, size_queue, is_trigger):
    if is_trigger == True:
        message = f":green_circle: `{ibc_route}` back to normal"
        client.loop.create_task(send_message_to_channel(message))
    update_ibc_routes(ibc_route, last_tx, counter, size_queue, False)

def get_ibc_status():
    print("GET IBC REMOTE")
    try:
        routes = requests.get(url, timeout=10).json()
        for route in routes:
            ibc_route = list(route.keys())[0]
            path_info = select_specific_ibc_routes(ibc_route)
            last_tx = route[ibc_route]["last_tx"]
            counter = route[ibc_route]["counter"]
            size_queue = route[ibc_route]["size_queue"]
            is_trigger = route[ibc_route]["is_trigger"]
            if check_ibc_routes(ibc_route):
                if path_info[ibc_route]["last_tx"] == last_tx:
                    update_ibc_routes(ibc_route, last_tx, counter, size_queue, is_trigger)
                else:
                    reinit_path_relay(ibc_route, last_tx, counter, size_queue, path_info[ibc_route]["is_trigger"])
            else:
                insert_ibc_routes(ibc_route, last_tx, counter, size_queue, is_trigger)
    except Exception:
        traceback.print_exc()


async def send_message_to_channel(message):
    channel_ibc_gang = client.get_channel(id=channel_id_ibc_gang)
    try:
        await channel_ibc_gang.send(message)
    except Exception:
        traceback.print_exc

async def ibc_status_execution():
    await client.wait_until_ready()
    while True:
        try:
            get_ibc_status()
            all_routes = select_ibc_routes()
            print(all_routes)
            for route in all_routes:
                print(route)
                ibc_route = list(route.keys())[0]
                last_tx = route[ibc_route]["last_tx"]
                counter = route[ibc_route]["counter"]
                size_queue = route[ibc_route]["size_queue"]
                is_trigger = route[ibc_route]["is_trigger"]
                if int(counter)%5 == 0 and int(counter) != 0: # check every 5min
                    update_ibc_routes(ibc_route, ibc_status.c.last_tx, ibc_status.c.counter, ibc_status.c.size_queue, True)
                    if int(counter) < 20:
                        message = f":yellow_circle: `{ibc_route}` in pending since `{format_timespan(int(counter)*60)}` with a queue size of `{size_queue}` packets `[Sequence:{last_tx}]`"
                    else:
                        message = f":red_circle: `{ibc_route}` in pending since `{format_timespan(int(counter)*60)}` with a queue size of `{size_queue}` packets `[Sequence:{last_tx}]`"
                    await send_message_to_channel(message)
            await asyncio.sleep(SCHEDULE_SECONDES) # task runs every 1 minutes
        except Exception:
            traceback.print_exc()
            pass

@client.event
async def on_ready():
    print('Logged in as')
    print(client.user.name)
    print(client.user.id)
    print('------')

def main():
    client.loop.create_task(ibc_status_execution())
    client.run(token_bot)

if __name__ == "__main__":
    main()
