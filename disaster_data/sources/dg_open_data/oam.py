import os
import json
import subprocess
from datetime import datetime
from multiprocessing.pool import ThreadPool

import boto3
import requests

from disaster_data.scraping import ScrapyRunner
from disaster_data.sources.dg_open_data.spider import DGOpenDataOAM

s3 = boto3.client('s3')
dg_api_key = os.environ['DG_API_KEY']
target_bucket = 'cognition-disaster-data'

def build_oam_catalog(id_list, verbose=False):

    DGOpenDataOAM.verbose = verbose

    with ScrapyRunner(DGOpenDataOAM) as runner:
        partial_items = runner.execute(ids=id_list, oam=True)
        complete_oam_items(partial_items)


def _complete_oam_item(partial_oam_item):
    # Get additional metadata from DG api
    # Upload oam item to S3
    url = "https://api.discover.digitalglobe.com/v1/services/ImageServer/query"
    headers = {
        "content-type": "application/x-www-form-urlencoded",
        'x-api-key': dg_api_key,
    }
    splits = partial_oam_item['title'].split('_')
    imgid = splits.pop(-1)
    event_name = '_'.join(splits)

    payload = {
        'outFields': '*',
        'outSR': '4326',
        'where': f"image_identifier IN ('{imgid}')",
        'returnGeometry': 'false',
        'f': 'json'
    }
    r = requests.post(url, headers=headers, data=payload)

    try:
        response = r.json()['features'][0]
    except:
        print("Received malformed response from DG api.")
        return None

    start_date = datetime.fromtimestamp(int(str(response['attributes']['collect_time_start'])[:-3])).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    end_date = datetime.fromtimestamp(int(str(response['attributes']['collect_time_end'])[:-3])).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    partial_oam_item.update({
        'acquisition_start': start_date,
        'acquisition_end': end_date,
        'sensor': response['attributes']['vehicle_name']
    })

    final_item = {
        "scenes": [
            partial_oam_item
        ]
    }

    # Upload to S3
    target_key = os.path.join('oam', event_name, imgid + '.json')
    print(f"Uploading OAM upload definition to s3://{target_bucket}/{target_key}")
    s3.put_object(Body=json.dumps(final_item), Bucket=target_bucket, Key=target_key)

def complete_oam_items(partial_oam_items):
    m = ThreadPool()
    m.map(_complete_oam_item, partial_oam_items)


def oam_upload(cookie, payload):
    resp = subprocess.call(f'curl -X POST --cookie "oam-session={cookie}" -H "Content-Type: application/json" -d @{payload} https://api.openaerialmap.org/uploads', shell=True)
    return resp