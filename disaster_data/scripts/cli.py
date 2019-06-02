import click

from disaster_data.sources.noaa_coast.spider import NoaaImageryCollections

@click.group()
def cognition_disaster_data():
    pass

@cognition_disaster_data.command(name="index-noaa-collections")
@click.option('--outfile', type=str, default='output.json')
@click.option('--id', type=str, multiple=True)
def index_noaa_collections(id, outfile):
    NoaaImageryCollections.crawl(outfile=outfile, ids=id)