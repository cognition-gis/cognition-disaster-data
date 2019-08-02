import json
import os
from concurrent.futures import ThreadPoolExecutor
from functools import partial

import boto3
from osgeo import gdal
from pyproj import Proj, transform
from shapely.ops import transform as reproject_geometry
from shapely.geometry import Polygon, mapping
from satstac import Collection
import requests
import utm

from disaster_data.scraping import ScrapyRunner
from disaster_data.sources.noaa_storm.spider import NoaaStormCatalog
from disaster_data.sources.noaa_storm.fgdc import parse_fgdc, temporal_window
from disaster_data.sources.noaa_storm import band_mappings

root_url = 'https://cognition-disaster-data.s3.amazonaws.com'
stac_updater_arn = 'arn:aws:lambda:us-east-1:725820063953:function:stac-updater-storm-dev-kickoff'
thumbnail_bucket = 'cognition-disaster-data'
thumbnail_key_prefix = 'thumbnails'

lambda_client = boto3.client('lambda')


def build_base_item(args):
    id = os.path.splitext(os.path.split(args['url'])[-1])[0]
    acq_date = args['url'].split('/')[-2].split('_')[0]
    datetime = f"{acq_date[0:4]}-{acq_date[4:6]}-{acq_date[6:8]}"

    partial_item = {
        'type': 'Feature',
        'id': id,
        'collection': args['event_name'],
        'properties': {
            'datetime': datetime,
            'eo:platform': 'aerial',
            'eo:instrument': 'TrimbleDSS',
            'eo:bands': band_mappings.DSS,
        },
        'assets': {
            "data": {
                "href": args['url'],
                "title": "Raster data",
                "type": "image/x.geotiff",
                "eo:bands": [
                    3,2,1
                ]
            },
            "metadata": {
                "href": args['metadata_url'],
                "title": "FGDC metadata",
                "type": "text/plain",
            },
            "thumbnail": {
                "href": "https://{}.s3.amazonaws.com/{}".format(
                    thumbnail_bucket, os.path.join(thumbnail_key_prefix, args['event_name'], datetime, id + '.jpg')
                ),
                "type": "image/jpeg",
                "title": "Thumbnail",
            }
        }
    }

    if 'world_file' in args:
        # Update id and datetime
        # Need to scrape datetime information
        partial_item['id'] = os.path.splitext(os.path.basename(args['url']))[0].split('-')[-1]
        partial_item['properties']['datetime'] = args['datetime']
        # Update assets
        partial_item['assets']['data'].update({
            "type": "image/jpeg"
        })
        partial_item['assets'].update({
            "worldfile": {
                "href": args["world_file"],
                "title": "Worldfile",
                "type": "text/plain"
            }
        })

    return partial_item

def append_gdal_info(item):
    info = gdal.Info(f"/vsitar//vsicurl/{item['assets']['data']['href']}", format='json', allMetadata=True, extraMDDomains='all')
    geometry = info['wgs84Extent']['coordinates']
    centroid = info['cornerCoordinates']['center']
    epsg = int(info['coordinateSystem']['wkt'].rsplit('"EPSG","', 1)[-1].split('"')[0])

    acq_date = info['metadata']['']['TIFFTAG_DATETIME']

    xvals = [x[0] for x in geometry[0]]
    yvals = [y[1] for y in geometry[0]]

    item.update({
        'bbox': [min(xvals), min(yvals), max(xvals), max(yvals)],
        'geometry': {
            'type': 'Polygon',
            'coordinates': geometry
        },
    })

    # Reproject to appropriate UTM zone via /vsimem/*.vrt to read spatial resolution in meters.
    utm_info = utm.from_latlon(*centroid[::-1])
    if centroid[1] > 0:
        utm_epsg = f'326{utm_info[2]}'
    else:
        utm_epsg = f'327{utm_info[2]}'

    try:
        warped_vrt = gdal.Warp(f'/vsimem/{item["id"]}.vrt', f"/vsitar//vsicurl/{item['assets']['data']['href']}", dstSRS=f'EPSG:{utm_epsg}')
        spatial_res = warped_vrt.GetGeoTransform()[1]
    finally:
        warped_vrt = None
        gdal.Unlink(f'/vsimem/{item["id"]}.vrt')

    item['properties'].update({
        'datetime': f"{acq_date[0:4]}-{acq_date[5:7]}-{acq_date[8:10]}T{acq_date[11:13]}:{acq_date[14:16]}:{acq_date[17:19]}.00Z",
        'eo:gsd': spatial_res,
        'eo:epsg': epsg
    })

    return item

def build_jpg_geometry(item):
    # Read information from stac item assets
    r = requests.get(item['assets']['worldfile']['href'])
    info = gdal.Info(f"/vsicurl/{item['assets']['data']['href']}", format='json', allMetadata=True, extraMDDomains='all')
    md = parse_fgdc(item['assets']['metadata']['href'])

    xres, ytilt, xtilt, yres, xmin, ymax = [float(x) for x in r.content.splitlines()] # xmin ymax
    xsize, ysize = info['size']
    xmax = xmin + (xres * xsize)
    ymin = ymax + (yres * ysize) #yres is negative
    geom = Polygon([[xmin, ymax], [xmax, ymax], [xmax, ymin], [xmin, ymin], [xmin, ymax]])

    # Reproject geometry
    epsg = f"326{md['UTMZoneNumber']}"
    in_proj = Proj(init=f"epsg:{epsg}")
    out_proj = Proj(init="epsg:4326")
    geom_proj = reproject_geometry(partial(transform, in_proj, out_proj), geom)

    item.update({
        'bbox': list(geom_proj.bounds),
        'geometry': json.loads(json.dumps(mapping(geom_proj)))
    })
    item['properties'].update({
        'eo:gsd': xres,
        'eo:epsg': int(epsg)
    })

def build_stac_item(args):
    stac_item = build_base_item(args)
    if args['url'].endswith('.jpg'):
        build_jpg_geometry(stac_item)
    else:
        append_gdal_info(stac_item)

    # Add to STAC-catalog with stac-updater
    lambda_client.invoke(
        FunctionName=stac_updater_arn,
        InvocationType="Event",
        Payload=json.dumps(stac_item)
    )


    return stac_item

def build_stac_items(organized_items):
    with ThreadPoolExecutor(max_workers=100) as executor:
        futures = []
        for coll in organized_items:
            for idx, url in enumerate(organized_items[coll]['urls']):
                args = {
                    'url': url,
                    'metadata_url': organized_items[coll]['metadata_url'],
                    'event_name': coll
                }

                if url.endswith('.jpg'):
                    args.update({
                        'world_file': organized_items[coll]['world_files'][idx],
                        'datetime': organized_items[coll]['datetimes'][idx]
                    })

                future = executor.submit(build_stac_item, args)
                futures.append(future)

        for future in futures:
            yield future.result()

def remote_listdir(dir):
    return [os.path.join(dir, x) for x in gdal.ReadDir(f"/vsitar//vsicurl/{dir}") if x.endswith('.tif')]

def get_url(item):
    if item['type'] == 'modern':
        print("Reading archive: {}".format(item['archive']))
        urls = remote_listdir(item['archive'])
        item.update({
            'urls': urls
        })
        return item
    return item

def get_urls(items):
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(get_url, x) for x in items]

        for future in futures:
            yield future.result()

def create_collections(collections, items, id_list):
    id_list = [x+'@storm' for x in id_list]
    noaa_collection = Collection.open(os.path.join(root_url, 'NOAAStorm', 'catalog.json'))
    current_cat_names = [x.split('/')[-2] for x in noaa_collection.links(rel='child')]

    # Build collection for each unique event, use FGDC metadata.
    # Create with sat-stac if not already exist.
    for id, coll in zip(id_list, collections):
        if coll['id'] not in current_cat_names:
            val = None
            # Search through the scraped items until we find one in the appropriate collection
            while not val:
                for item in items:
                    if item['event_name'] in id:
                        val = item

            # Read FGDC metadata
            md = parse_fgdc(val['metadata_url'])

            coll.update({
                'title': md['Title'],
                'description': md['Abstract'] + '. '.join(md['Purpose'].split('.')[1:]),
                "extent": {
                    "spatial": [
                        float(md['WestBoundingCoordinate']),
                        float(md['SouthBoundingCoordinate']),
                        float(md['EastBoundingCoordinate']),
                        float(md['NorthBoundingCoordinate'])
                    ],
                    "temporal": temporal_window(md)
                },
                "keywords": [x.lower() for x in md['ThemeKeyword'][:-1] if x]
            })

            new_coll = Collection(coll)
            noaa_collection.add_catalog(new_coll)
            noaa_collection.save()

def organize_by_collection(items):
    organized = {}
    for item in items:
        if item['event_name'] not in organized:
            organized.update({
                item['event_name']: {
                    'metadata_url': item['metadata_url'],
                    'urls': item['urls']
                }
            })
            if item['type'] == 'old':
                organized[item['event_name']].update({
                    'world_files': [item['world_file']],
                    'datetimes': [item['datetime']]
                })
        else:
            for url in item['urls']:
                organized[item['event_name']]['urls'].append(url)
                if item['type'] == 'old':
                    organized[item['event_name']]['world_files'].append(item['world_file'])
                    organized[item['event_name']]['datetimes'].append(item['datetime'])

    return organized

def build_stac_catalog(id_list=None, limit=None, collections_only=False, verbose=False):

    NoaaStormCatalog.verbose = verbose

    with ScrapyRunner(NoaaStormCatalog) as runner:
        scraped_items = list(runner.execute(ids=id_list))
        collections = scraped_items.pop(0)
        item_count = scraped_items.pop(0)

        create_collections(collections, scraped_items, id_list)

        if collections_only:
            return

        items_with_urls = get_urls(scraped_items)
        organized = organize_by_collection(items_with_urls)

        if limit:
            for coll in organized:
                organized[coll]['urls'] = organized[coll]['urls'][:limit]

        stac_items = build_stac_items(organized)
        for item in stac_items:
            print(item)

build_stac_catalog(id_list='hurricane-barry', limit=5)