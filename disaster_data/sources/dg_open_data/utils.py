import os
import json
import tempfile
import shutil
import uuid
from datetime import datetime
from multiprocessing.pool import ThreadPool
import subprocess

import requests
from satstac import Catalog, Collection, Item

from disaster_data.scraping import ScrapyRunner
from disaster_data.catalog.catalog import organize_stac_assets
from disaster_data.utils import gdal_info_stac_multi
from disaster_data.sources.dg_open_data.spider import DGOpenDataCollections
from . import band_mappings

root_url = 'https://cognition-disaster-data.s3.amazonaws.com'
oam_upload_url = 'https://api.openaerialmap.org/uploads'
oam_cookie = os.environ['OAM_COOKIE']

stac_mapping = {
    'sun_elevation_avg': 'eo:sun_azimuth',
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

def append_dg_metadata(stac_payload):
    url = "https://api.discover.digitalglobe.com/v1/services/ImageServer/query"
    headers = {
        "content-type": "application/x-www-form-urlencoded",
        'x-api-key': "9xJw9yMzlS7lXWYRrgTA64cQXBcd5T2v3GldM3sY",
    }
    stac_item = stac_payload['item']
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
    response = r.json()

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
                dg_props.update({f"dg:{k}": v})
        stac_item['properties'].update(dg_props)

        # Add browse url to assets
        stac_item['assets'].update({
            'browseURL': {
                'href': stac_item['properties'].pop('dg:browse_url')
            }
        })

        # Add band mappings
        stac_item['properties'].update({'eo:bands': getattr(band_mappings, stac_item['properties']['eo:platform'])})

        # Use panchromatic resolution over ms resolution if WV01
        if stac_item['properties']['eo:platform'] == 'WV01':
            stac_item['properties'].update({'eo:gsd': stac_item['properties']['dg:pan_resolution_avg']})

        # Handling datetime
        collect_time_start = datetime.fromtimestamp(int(str(stac_item['properties']['dg:collect_time_start'])[:-3])).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        collect_time_end = datetime.fromtimestamp(int(str(stac_item['properties']['dg:collect_time_end'])[:-3])).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        stac_item['properties']['dg:collect_time_start'] = collect_time_start
        stac_item['properties']['dg:collect_time_end'] = collect_time_end
        stac_item['properties']['datetime'] = collect_time_start

        return {'parent': stac_payload['parent'], 'item': stac_item}
    else:
        # Return partial stac item (with geo-information) in event of bad query.
        return {'parent': stac_payload['parent'], 'item': stac_item}

def append_dg_metadata_multi(stac_payloads, num_threads=10):
    m = ThreadPool(num_threads)
    response = m.map(append_dg_metadata, stac_payloads)
    m.close()
    m.join()
    return response

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
    return resp.json()


def build_dg_catalog(id_list, num_threads=10, limit=None, stac=True, oam=False):
    if oam:
        stac = False

    with ScrapyRunner(DGOpenDataCollections) as runner:
        print("Scraping initial STAC information from url.")
        response = runner.execute(ids=id_list, items=True)
        organized = organize_stac_assets(response)


        base_items = []
        for col_id in organized['items']:
            if limit:
                organized['items'][col_id] = organized['items'][col_id][:limit]
            for partial_item in organized['items'][col_id]:
                base_items.append({'parent': col_id, 'item': partial_item})

        print("Found {} unique assets within collections: {}".format(len(base_items), id_list))
        print("Reading geometry for each item with gdal.Info.")
        partial_items = gdal_info_stac_multi(base_items, num_threads)

        print("Reading metadata for each item from DG Browse API.")
        complete_items = append_dg_metadata_multi(partial_items)

        print(json.dumps(complete_items[0], indent=1))

        if stac:
            print("Building STAC catalog.")
            datasource_coll = Collection.open(os.path.join(root_url, 'DGOpenData', 'catalog.json'))

            for coll in list(organized['collections']):
                new_coll = Collection(organized['collections'][coll])
                datasource_coll.add_catalog(new_coll)
                for item in complete_items:
                    if item['parent'] == coll:
                        stac_item = Item(item['item'])
                        new_coll.add_item(stac_item)

        if oam:
            # Map STAC items to OAM items
            oam_scenes = stac_to_oam(complete_items)
            # Create payload and upload to OAM
            tempdir = tempfile.mkdtemp()
            temp_fpath = os.path.join(tempdir, str(uuid.uuid4()))
            with open(temp_fpath, 'w') as outfile:
                json.dump(oam_scenes, outfile, indent=2)

            oam_upload(oam_cookie, temp_fpath)
