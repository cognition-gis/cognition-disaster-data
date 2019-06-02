import json

import click

from disaster_data.sources.noaa_coast.spider import NoaaImageryCollections
from disaster_data.utils import gdal_info_stac_multi
from disaster_data.scraping import ScrapyRunner

@click.group()
def cognition_disaster_data():
    pass

@cognition_disaster_data.command(name="index-noaa-collections")
@click.option('--outfile', type=str, default='output.json')
@click.option('--id', type=str, multiple=True)
@click.option('--items/--no-items', default=False)
def index_noaa_collections(id, outfile, items):
    # NoaaImageryCollections.crawl(outfile=outfile, ids=id)

    if len(id) == 0:
        id = None

    with ScrapyRunner(NoaaImageryCollections) as runner:
        response = runner.execute(ids=id, items=items)[0]

        if items:
            response['items'] = gdal_info_stac_multi(response['items'][1], num_threads=25)

        with open(outfile, 'w') as _outfile:
            json.dump(response, _outfile)