import falcon
import json
import simplejson
import boto3
import subprocess
import datetime
import time
import os
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



def all_status():
    ts = int(time.time())
    date_day = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d')

    scan_items = table.scan(
        FilterExpression=Attr('date').eq(date_day)
    )

    all_statusData = {
        "Count": str(scan_items['Count']),
        "Items": simplejson.dumps(scan_items['Items'])
    }
    
    return all_statusData

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

class statusgeneral:
    def on_get(self, req, resp):

        data = all_status()

        message = { 
            'message': {
                'count': data["Count"],
                'items': data["Items"]
            }
        }

        resp.body = json.dumps(message)
        resp.status = falcon.HTTP_200
        
api = falcon.API(middleware=[cors.middleware])
api.add_route('/homemonitor', homemonitor())
api.add_route('/statusmonitor', statusmonitor())
api.add_route('/statusgeneral', statusgeneral())
api.add_route('/statussingle/{id_station}', statussingle())