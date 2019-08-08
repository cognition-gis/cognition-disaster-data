import os
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import multiprocessing
import tempfile
import subprocess

from satstac import Collection, Catalog, Item

from disaster_data.scraping import ScrapyRunner
from disaster_data.sources.noaa_storm.spider import NoaaStormCatalog
from disaster_data.sources.noaa_storm.fgdc import parse_fgdc, temporal_window
from disaster_data.sources.noaa_storm.assets import ObliqueArchive, RGBArchive, JpegTilesArchive

ROOT_URL = 'https://cognition-disaster-data.s3.amazonaws.com'
NOAA_STORM_ROOT = 'https://cognition-disaster-data.s3.amazonaws.com/NOAAStorm'
MAX_THREADS = int(os.environ.get("MAX_THREADS", multiprocessing.cpu_count() * 5))

def load_datetime(date_str):
    try:
        return datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S.%fZ")
    except:
        return datetime.strptime(date_str, "%Y-%m-%d")

def download_archives(archives, out_dir):
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        [executor.submit(x.download(out_dir=out_dir), x) for x in archives]
    return

def _build_stac_items(asset):
    return asset.build_items()

def build_stac_items(assets, thumbdir):
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = [executor.submit(_build_stac_items, x) for x in assets]
        for future in futures:
            for item in future.result():
                yield item

def build_thumbnails(archives, thumbdir):
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        [executor.submit(x.build_thumbnails(dir=os.path.join(thumbdir, x.item['event_name'])), x) for x in archives]
    return

def create_collections(collections, items, id_list):
    id_list = [x+'@storm' for x in id_list]
    out_collections = []

    # Build collection for each unique event, use FGDC metadata.
    # Create with sat-stac if not already exist.
    for id, coll in zip(id_list, collections):
        val = None
        # Search through the scraped items until we find one in the appropriate collection
        while not val:
            for item in items:
                if item['event_name'] in id:
                    val = item

        # Read FGDC metadata
        try:
            md = parse_fgdc(val['metadata_url'])
        except:
            coll.update({
                'extent': {
                    'spatial': [],
                    'temporal': []
                }
            })
            return [coll]

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

        # Sometimes FGDC will only list year/month for dates
        # If this happens, use the date from a stac item as the starting extent.
        # Extent will be updated automatic ally as items are ingested with sat-stac
        if coll['extent']['temporal'][0].endswith('-') or coll['extent']['temporal'][1].endswith('-'):
            if val['archive'].endswith('GCS_NAD83.tar'):
                coll['extent']['temporal'][0] += val['archive'].split('/')[-1].split('_')[0][6:8]
                coll['extent']['temporal'][1] += val['archive'].split('/')[-1].split('_')[0][6:8]
            elif val['archive'].endswith('Oblique.tar'):
                coll['extent']['temporal'][0] += val['archive'].split('/')[-1][6:8]
                coll['extent']['temporal'][1] += val['archive'].split('/')[-1][6:8]
            else:
                coll['extent']['temporal'][0] += val['archive'].split('/')[-1].split('_')[0][-3:-1]
                coll['extent']['temporal'][1] += val['archive'].split('/')[-1].split('_')[0][-3:-1]
        out_collections.append(coll)
    return out_collections

def build_stac_catalog(id_list=None, verbose=False):

    tempdir = tempfile.mkdtemp(prefix='/home/slingshot/Downloads/')
    tempthumbs = tempfile.mkdtemp(prefix='/home/slingshot/Downloads/')

    NoaaStormCatalog.verbose = verbose

    print("Running web scraper.")
    with ScrapyRunner(NoaaStormCatalog) as runner:
        scraped_items = list(runner.execute(ids=id_list))
        collections = scraped_items.pop(0)
        item_count = scraped_items.pop(0)

        collections = create_collections(collections, scraped_items, id_list)

        # Build stac catalog locally
        # Start with NOAA Storm catalog
        root_catalog = Catalog.open(os.path.join(ROOT_URL, 'NOAAStorm', 'catalog.json'))
        root_catalog.save_as(filename=os.path.join(tempdir, 'catalog.json'))

        print("Creating collections.")
        # Create collections
        d = {}
        for collection in collections:
            coll = Collection(collection)
            root_catalog.add_catalog(coll)
            d.update({
                collection['id']: coll
            })

        # Setup directories for thumbnails
        thumbdir = os.path.join(tempthumbs, 'thumbnails')
        os.mkdir(thumbdir)
        for coll in d:
            coll_dir = os.path.join(thumbdir, d[coll].id)
            if not os.path.exists(coll_dir):
                os.mkdir(coll_dir)

        # Sort assets
        archive_assets = []
        for item in scraped_items:
            if 'archive' in item:
                if item['archive'].endswith('_RGB.tar'):
                    archive_assets.append(RGBArchive(item, os.path.join(thumbdir, d[item['event_name']].id)))
                elif item['archive'].endswith(('GCS_NAD83.tar', 'GCS_NAD83.zip')):
                    archive_assets.append(JpegTilesArchive(item, os.path.join(thumbdir, d[item['event_name']].id)))
                elif item['archive'].endswith(('Oblique.tar', 'Oblique.zip')):
                    archive_assets.append(ObliqueArchive(item, os.path.join(thumbdir, d[item['event_name']].id)))
            else:
                print("Found a JPG with disconnected world file")

        # Download archives
        download_archives(archive_assets, '/home/slingshot/Downloads')

        print("Creating items and thumbnails.")
        # Add items
        for item in build_stac_items(archive_assets, thumbdir):
            d[item['collection']].add_item(Item(item), path='${date}', filename='${id}')

            # Update spatial extent of collection
            try:
                if item['bbox'][0] < d[item['collection']].extent['spatial'][0]:
                    d[item['collection']].extent['spatial'][0] = item['bbox'][0]
                if item['bbox'][1] < d[item['collection']].extent['spatial'][1]:
                    d[item['collection']].extent['spatial'][1] = item['bbox'][1]
                if item['bbox'][2] < d[item['collection']].extent['spatial'][2]:
                    d[item['collection']].extent['spatial'][2] = item['bbox'][2]
                if item['bbox'][3] < d[item['collection']].extent['spatial'][3]:
                    d[item['collection']].extent['spatial'][3] = item['bbox'][3]
            except:
                d[item['collection']].extent['spatial'] =  item['bbox']

            # Update temporal extent of collection
            try:
                item_dt = load_datetime(item['properties']['datetime'])
                min_dt = load_datetime(d[item['collection']].extent['temporal'][0])
                max_dt = load_datetime(d[item['collection']].extent['temporal'][1])
                if item_dt < min_dt:
                    d[item['collection']].extent['temporal'][0] = item['properties']['datetime']
                if item_dt > max_dt:
                    d[item['collection']].extent['temporal'][1] = item['properites']['datetime']
            except:
                d[item['collection']].extent['temporal'] = [item['properties']['datetime'], item['properties']['datetime']]

        # Upload catalog to S3
        print("Uploading catalog to S3.")
        subprocess.call(f"aws s3 sync {tempdir} s3://cognition-disaster-data/NOAAStorm/", shell=True)

        print("Uploading thumbnails to S3.")
        # Upload thumbnails to S3
        subprocess.call(f"aws s3 sync {thumbdir} s3://cognition-disaster-data/thumbnails/", shell=True)


build_stac_catalog(['illinois-tornadoes'])