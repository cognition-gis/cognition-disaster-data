import json
import os
from concurrent.futures import ThreadPoolExecutor

from osgeo import gdal
from satstac import Collection
import utm

from disaster_data.scraping import ScrapyRunner
from disaster_data.sources.noaa_storm.spider import NoaaStormCatalog
from disaster_data.sources.noaa_storm.fgdc import parse_fgdc

root_url = 'https://cognition-disaster-data.s3.amazonaws.com'

def build_base_item(args):
    id = os.path.splitext(os.path.split(args['url'])[-1])[0]
    acq_date = args['url'].split('/')[-2].split('_')[0]

    partial_item = {
        'type': 'Feature',
        'id': id,
        'collection': args['event_name'],
        'properties': {
            'datetime': f"{acq_date[0:4]}-{acq_date[4:6]}-{acq_date[6:8]}",
        },
        'assets': {
            "data": {
                "href": args['url'],
                "title": "Raster data",
                "type": "image/x.geotiff"
            },
            "metadata": {
                "href": args['metadata_url'],
                "title": "FGDC metadata",
                "type": "text/plain"
            }
        }
    }

    return partial_item

def append_gdal_info(item):
    info = gdal.Info(f"/vsitar//vsicurl/{item['assets']['data']['href']}", format='json')
    geometry = info['wgs84Extent']['coordinates']
    centroid = info['cornerCoordinates']['center']
    epsg = int(info['coordinateSystem']['wkt'].rsplit('"EPSG","', 1)[-1].split('"')[0])

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
        'eo:gsd': spatial_res,
        'eo:epsg': epsg
    })

    return item

def build_stac_item(args):

    if args['url'].endswith('.jpg'):
        print("Found old image, check for world file.")
        pass
    else:
        base_item = build_base_item(args)
        append_gdal_info(base_item)
        return base_item


def build_stac_items(organized_items):
    with ThreadPoolExecutor(max_workers=100) as executor:
        futures = []
        for coll in organized_items:
            for url in organized_items[coll]['urls']:
                args = {
                    'url': url,
                    'metadata_url': organized_items[coll]['metadata_url'],
                    'event_name': coll
                }
                future = executor.submit(build_stac_item, args)
                futures.append(future)

        for future in futures:
            yield future.result()

def remote_listdir(dir):
    return [os.path.join(dir, x) for x in gdal.ReadDir(f"/vsitar//vsicurl/{dir}")]

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
    noaa_collection = Collection.open(os.path.join(root_url, 'NOAAStorm', 'catalog.json'))
    current_cat_names = [x.split('/')[-2] for x in noaa_collection.links(rel='child')]

    # Build collection for each unique event, use FGDC metadata.
    # Create with sat-stac if not already exist.
    for id, coll in zip(id_list, collections):
        if coll['id'] not in current_cat_names:
            val = None
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
                },
                "keywords": [x.lower() for x in md['ThemeKeyword'][:-1] if x]
            })

            print(json.dumps(coll, indent=2))


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
        else:
            for url in item['urls']:
                organized[item['event_name']]['urls'].append(url)

    return organized


def build_stac_catalog(id_list=None, limit=None, collections_only=False):


    with ScrapyRunner(NoaaStormCatalog) as runner:
        scraped_items = list(runner.execute(ids=id_list))
        collections = scraped_items.pop(0)
        item_count = scraped_items.pop(0)

        create_collections(collections, scraped_items, id_list)

        if collections_only:
            return

        if limit:
            scraped_items = scraped_items[:limit]

        items_with_urls = get_urls(scraped_items)
        organized = organize_by_collection(items_with_urls)


        stac_items = build_stac_items(organized)

build_stac_catalog(['hurricane-barry'], limit=1)
