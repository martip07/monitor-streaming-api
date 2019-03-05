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

def merge_lists(l1, l2, key):
    merged = {}
    for item in l1+l2:
        if item[key] in merged:
            merged[item[key]].update(item)
        else:
            merged[item[key]] = item
    return [val for (_, val) in merged.items()]

def all_ordered():
    ts = int(time.time())
    date_day = datetime.datetime.fromtimestamp(ts).strftime('2019-02-28')

    scan_items = table.scan(
        FilterExpression=Attr('date').eq(date_day)
    )

    all_statusData = {
        "Count": str(scan_items['Count']),
        "Items": simplejson.dumps(scan_items['Items'])
    }

    items = json.loads(simplejson.dumps(scan_items['Items']))

    providers_list = []

    for provider in items:
        providers_list.append(provider["provider"])

    provider_list = set(providers_list)
    
    print(provider_list)

    provider_item = list()
    provider_data = list()

    for item in items:
        for provider in provider_list:
            if provider in item["provider"]:
                #print("Same provider: " + provider)
                data = {
                    'provider': provider,
                    'items': [item["station_id"]]
                }
                provider_item.append(data)
                #return provider_item
            else:
                #print("Not the same provider")
                pass

    #print(provider_item)

    for i,item in enumerate(provider_item):
        if provider == provider_item[i]["provider"]:
            new_item = provider_item[i]["items"]
            #print(new_item[0])
            provider_item[i]["items"].append(new_item[0])
    #print(provider_item)

    for provider in provider_list:
        print(provider)
        scan_items = table.scan(
            FilterExpression=Attr('provider').eq(provider) & Attr('date').eq(date_day)
        )

        data = {
            'count': str(scan_items['Count']),
            'provider': provider,
            'items': json.loads(simplejson.dumps(scan_items['Items']))
        }

        provider_data.append(data)


    #if provider_list.

    return provider_data

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
api.add_route('/statushome', statushome())
api.add_route('/statussingle/{id_station}', statussingle())