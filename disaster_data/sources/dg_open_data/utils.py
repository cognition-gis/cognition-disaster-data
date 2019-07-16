import os
import json
import math
from datetime import datetime
from multiprocessing import Process, Pipe
import subprocess
import itertools

import boto3
import requests
from satstac import Collection, Item

from disaster_data.scraping import ScrapyRunner
from disaster_data.sources.dg_open_data.spider import DGOpenDataCatalog, DGOpenDataSummary
from . import band_mappings

from osgeo import gdal

lambda_client = boto3.client('lambda')
s3_client = boto3.client('s3')

root_url = 'https://cognition-disaster-data.s3.amazonaws.com'
oam_upload_url = 'https://api.openaerialmap.org/uploads'
thumbnail_bucket = 'cognition-disaster-data'
thumbnail_key_prefix = 'thumbnails'
thumbnail_kickoff_bucket = 'cognition-thumbnails-kickoff'
stac_updater_arn = 'arn:aws:lambda:us-east-1:725820063953:function:stac-updater-dev-kickoff'

oam_cookie = os.environ['OAM_COOKIE']
dg_api_key = os.environ['DG_API_KEY']

stac_mapping = {
    'sun_elevation_avg': 'eo:sun_elevation',
    'sun_azimuth_avg': 'eo:sun_azimuth',
    'target_azimuth_avg': 'eo:azimuth',
    'area_avg_off_nadir_angle': 'eo:off_nadir',
    'area_cloud_cover_percentage': 'eo:cloud_cover',
    'vehicle_name': 'eo:platform',
    'sensor_name': 'eo:instrument',
    'multi_resolution_avg': 'eo:gsd'
}

sensor_mapping = {
    'GE01': 'GeoEye-1',
    'WV01': 'WorldView-1',
    'WV02': 'WorldView-2',
    'WV03': 'WorldView-3',
    'WV04': 'WorldView-4'
}

# List of relevant dg keys to include in item
# These do not fall into any standard STAC extension (ex. eo) but are still pertinent to search / discovery
relevant_dg_keys = [
    'collect_time_start',
    'collect_time_end',
    'image_identifier',
    'legacy_identifier_reference',
    'multi_resolution_max',
    'multi_resolution_min',
    'browse_url',
    'objectid',
    'dg:relative_geolocation_accuracy',
]

def append_dg_metadata(stac_item):
    url = "https://api.discover.digitalglobe.com/v1/services/ImageServer/query"
    headers = {
        "content-type": "application/x-www-form-urlencoded",
        'x-api-key': dg_api_key,
    }
    imgid = stac_item['assets']['data']['href'].split('/')[-2]

    xvals = [x[0] for x in stac_item['geometry']['coordinates'][0]]
    yvals = [y[1] for y in stac_item['geometry']['coordinates'][0]]

    payload = {
        'outFields': '*',
        'geometryType': 'esriGeometryEnvelope',
        'geometry': f"{min(xvals)},{min(yvals)},{max(xvals)},{max(yvals)}",
        'outSR': '4326',
        'where': f"image_identifier IN ('{imgid}')",
        'performAreaBasedCalc': 'true',
        'returnGeometry': 'false',
        'f': 'json'
    }


    r = requests.post(url, headers=headers, data=payload)

    try:
        response = r.json()
    except:
        print("Received malformed response from DG api.")
        return None

    # Handle for bad queries
    if len(response['features']) > 0:
        feature = response['features'][0]
        stac_keys = list(stac_mapping)
        dg_props = {}

        # Create STAC properties from DG metadata
        for (k,v) in feature['attributes'].items():
            if k in stac_keys:
                dg_props.update({stac_mapping[k]: v})
            else:
                if k in relevant_dg_keys:
                    dg_props.update({f"dg:{k}": v})

        # If the dg asset doesn't have a legacy identifier it can't be indexed
        if not feature['attributes']['legacy_identifier_reference']:
            return None

        stac_item['properties'].update(dg_props)

        # Add band mappings
        band_count = stac_item['properties'].pop('bandcount')
        try:
            stac_item['properties'].update({'eo:bands': getattr(band_mappings, stac_item['properties']['eo:platform'])})
        except:
            print("Failed band mapping. The {} instrument is not registered.".format(stac_item['properties']['eo:platform']))
        stac_item['assets']['data'].update({
            'eo:bands': list(range(band_count))
        })

        # Use panchromatic resolution over ms resolution if WV01
        if stac_item['properties']['eo:platform'] == 'WV01':
            stac_item['properties'].update({'eo:gsd': feature['attributes']['pan_resolution_avg']})

        # Handling datetime
        collect_time_start = datetime.fromtimestamp(int(str(stac_item['properties']['dg:collect_time_start'])[:-3])).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        collect_time_end = datetime.fromtimestamp(int(str(stac_item['properties']['dg:collect_time_end'])[:-3])).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        stac_item['properties']['dg:collect_time_start'] = collect_time_start
        stac_item['properties']['dg:collect_time_end'] = collect_time_end
        stac_item['properties']['datetime'] = collect_time_start
    else:
        print("Bad query on DG image {}_{}".format(imgid, stac_item['id']))
        # Parse necessary information from filename
        # Return as partial item (without DG metadata appended)
        splits = stac_item['assets']['data']['href'].split('/')
        stac_item['properties'].update({
            'dg:legacy_identifier_reference': splits[-2],
            'datetime': splits[-3],
        })

    thumbnail_key = os.path.join(thumbnail_key_prefix,
                                 stac_item['collection'],
                                 stac_item['properties']['datetime'].split('T')[0],
                                 stac_item['properties']['dg:legacy_identifier_reference'],
                                 stac_item['id'] + '.jpg')
    stac_item['assets'].update({
        'thumbnail': {
            'href': 'https://{}.s3.amazonaws.com/{}'.format(thumbnail_bucket, thumbnail_key),
            'type': 'image/jpeg'
        }
    })

    return stac_item

def append_gdal_info(partial_item):
    file_url = os.path.join("/vsicurl/" + partial_item['assets']['data']['href'])
    try:
        info = gdal.Info(file_url, format='json', allMetadata=True)
    except:
        print("Failed to read spatial information for file: {}".format(file_url))
        return None

    # Calculating geometry and bbox
    geometry = info['wgs84Extent']['coordinates']
    xvals = [x[0] for x in geometry[0]]
    yvals = [y[1] for y in geometry[0]]

    partial_item.update({
        'bbox': [min(xvals), min(yvals), max(xvals), max(yvals)],
        'geometry': {
            'type': 'Polygon',
            'coordinates': geometry
        }
    })
    partial_item['properties'].update({
        'eo:epsg': info['coordinateSystem']['wkt'].rsplit('"EPSG","', 1)[-1].split('"')[0],
        'eo:gsd': (info['geoTransform'][1] + abs(info['geoTransform'][-1])) / 2,
        'bandcount': len(info['bands'])
    })
    return partial_item

def _complete_stac_item(partial_stac_items, conn):

    for partial_item in partial_stac_items:
        # Append metadata to stac item with GDAL and DG Browse API
        _ = append_gdal_info(partial_item)
        if _:
            _ = append_dg_metadata(partial_item)
            if _:
                # Order properties keys alphabetically for nicer viewing with sat-browser
                partial_item['properties'] = dict(sorted(partial_item['properties'].items(), key=lambda x: x[0].lower()))

                # Add to stac catalog with stac-updater
                lambda_client.invoke(
                    FunctionName=stac_updater_arn,
                    InvocationType="Event",
                    Payload=json.dumps(partial_item)
                )

    conn.send([x for x in partial_stac_items if x])
    conn.close()

def complete_stac_items(partial_stac_items, batch_size, num_threads):
    parent_connections = []
    processes = []
    for thread in range(num_threads):
        parent_conn, child_conn = Pipe()
        parent_connections.append(parent_conn)

        process = Process(target=_complete_stac_item, args=(list(itertools.islice(partial_stac_items, batch_size)), child_conn))
        processes.append(process)

    print("Starting processes")
    for process in processes:
        process.start()

    print("Joining processes")
    for process in processes:
        process.join()

    print("Getting results from processes")
    for parent_connection in parent_connections:
        # Calling recv() on each item will begin passing it through pipeline.
        parent_connection.recv()

def create_collections(collections):
    dg_collection = Collection.open(os.path.join(root_url, 'DGOpenData', 'catalog.json'))

    # Create collections if not exist
    current_cat_names = [x.split('/')[-2] for x in dg_collection.links(rel='child')]

    out_d = {}
    for coll in collections:
        if coll['id'] not in current_cat_names:
            print("Creating new collection: {}".format(coll['id']))
            new_coll = Collection(coll)
            dg_collection.add_catalog(new_coll)
            out_d.update({coll['id']: new_coll})
            dg_collection.save()
        else:
            print("Opening existing collection: {}".format(coll['id']))
            out_d.update({coll['id']:Collection.open(os.path.join(root_url, 'DGOpenData', coll['id'], 'catalog.json'))})


    return out_d

def stac_to_oam(stac_items):
    # Sort STAC items by item ID.
    added_ids = []
    sorted_items = {}
    for item in stac_items:
        item_id = item['item']['assets']['data']['href'].split('/')[-2]
        if item_id not in added_ids:
            sorted_items.update({item_id: [item]})
            added_ids.append(item_id)
        else:
            sorted_items[item_id].append(item)

    oam_scenes = {
        'scenes': []
    }

    for item_id in sorted_items:
        # Aggregate min/max time from all items associated with the id
        start_times = [x['item']['properties']['dg:collect_time_start'] for x in sorted_items[item_id] if 'dg:collect_time_start' in x['item']['properties'].keys()]
        end_times = [x['item']['properties']['dg:collect_time_end'] for x in sorted_items[item_id] if 'dg:collect_time_end' in x['item']['properties'].keys()]
        combined_times = start_times + end_times

        # Grab urls from all items associated with the id
        id_urls = [x['item']['assets']['data']['href'] for x in sorted_items[item_id]]
        oam_item = {
            "title": item_id,
            "provider": "Digital Globe Open Data Program",
            "platform": "satellite",
            "lisence": "CC BY-NC 4.0",
            "sensor": sensor_mapping[sorted_items[item_id][0]['item']['properties']['eo:platform']],
            "acquisition_start": min(combined_times),
            "acquisition_end": max(combined_times),
            "urls": id_urls
        }
        oam_scenes['scenes'].append(oam_item)

    return oam_scenes

def oam_upload(cookie, payload):
    resp = subprocess.call(f'curl -H "cookie: {cookie}" -H "Content-Type: application/json" -d @{payload} https://api.openaerialmap.org/uploads', shell=True)
    return resp

def build_dg_catalog(id_list, num_threads=10, limit=None, collections_only=False, verbose=False):

    DGOpenDataCatalog.verbose = verbose

    with ScrapyRunner(DGOpenDataCatalog) as runner:

        partial_items = runner.execute(ids=id_list, items=True)
        collections = next(partial_items)
        item_count = next(partial_items)

        if limit:
            item_count = limit

        batch_size = int(math.ceil(item_count / num_threads))
        print("Item count: {}".format(item_count))
        print("Batch size: {}".format(batch_size))

        # Build and ingest stac collections
        create_collections(collections)

        if collections_only:
            return

        # Build and ingest stac items
        complete_stac_items(partial_items, batch_size, num_threads)
        print("Finished building STAC items.")
