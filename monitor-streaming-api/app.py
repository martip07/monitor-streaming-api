import falcon
import json
import simplejson
import boto3
import subprocess
import datetime
import time
import os
from rethinkdb import RethinkDB
from boto3.dynamodb.conditions import Key, Attr
from falcon_cors import CORS


#### GENERAL CONFIG ####

with open('./config/config.json') as config_file:
    data_config = json.load(config_file)

env_app = os.getenv('GUARDIAN_SERVER_ENV')

cors = CORS(allow_origins_list=data_config[env_app]['CORS-IPS']) 
#######

#### DYNAMODB CONFIG ####

dynamodb = boto3.resource('dynamodb', region_name=data_config[env_app]['AWS-REGION'])
table = dynamodb.Table(data_config[env_app]['DYNAMO-TABLE'])
#######


#### RETHINKDB CONFIG ####

drt = RethinkDB()
connection = drt.connect(db=data_config[env_app]['RETHINK-DB'])
#######

def single_provider(provider_query, date_query):
    data_provider = list()
    data_item = list()
    cursor_provider = drt.table(data_config[env_app]['RETHINK-TABLE']).filter((drt.row["provider"] == provider_query) & (drt.row["date"] == date_query)).run(connection)
    for document in cursor_provider:
        data_item.append(document)
    item = {
        'provider': provider_query,
        'items': data_item
    }

    return item

def all_status():
    ts = int(time.time())
    date_day = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d')

    cursor = drt.table(data_config[env_app]['RETHINK-TABLE']).filter(drt.row["date"] == date_day).run(connection)

    items_array = list()

    for document in cursor:
        items_array.append(document)

    all_statusData = {
        "Items": json.loads(simplejson.dumps(items_array))
    }
  
    return all_statusData

def all_ordered():
    ts = int(time.time())
    date_day = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d')

    cursor = drt.table(data_config[env_app]['RETHINK-TABLE']).filter(drt.row["date"] == date_day).run(connection)

    items_array = list()

    for document in cursor:
        items_array.append(document)

    items = json.loads(simplejson.dumps(items_array))

    providers_list = []

    for provider in items:
        providers_list.append(provider["provider"])

    provider_list = set(providers_list)
    
    print(provider_list)

    provider_item = list()
    provider_data = list()
    data_list = list()

    
    for provider in provider_list:
        single_item = single_provider(provider, date_day)
        data_list.append(single_item)

    return data_list


class homemonitor:
    def on_get(self, req, resp):
        message = {
            'data': 'Servicio de monitoreo de streaming',
            'status': 200
        }
        
        resp.body = json.dumps(message)
        resp.status = falcon.HTTP_202

class statusmonitor:
    def on_get(self, req, resp):
        message = {
            'message': 'status monitor uri',
            'status': 200
        }

        resp.body = json.dumps(message)
        resp.status = falcon.HTTP_202

    def on_post(self, req, resp):

        message = {
            'message': {
                'data': str(99)
            },
            'status': 202
        }

        resp.body = json.dumps(message)
        resp.status = falcon.HTTP_202

class statussingle:
    def on_get(self, req, resp, id_station):
        data = str(id_station)

        message = {
            'message': data
        }
        resp.body = json.dumps(message)
        resp.status = falcon.HTTP_202

class statushome:
    def on_get(self, req, resp):

        data = all_ordered()

        message = { 
            'message': data
        }

        resp.body = json.dumps(message)
        resp.status = falcon.HTTP_200

class statusgeneral:
    def on_get(self, req, resp):

        data = all_status()

        print(data)

        message = { 
            'message': data
        }

        resp.body = json.dumps(message)
        resp.status = falcon.HTTP_200
        
api = falcon.API(middleware=[cors.middleware])
api.add_route('/homemonitor', homemonitor())
api.add_route('/statusmonitor', statusmonitor())
api.add_route('/statusgeneral', statusgeneral())
api.add_route('/statushome', statushome())
api.add_route('/statussingle/{id_station}', statussingle())