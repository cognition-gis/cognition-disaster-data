import click

from disaster_data.sources.noaa_storm import noaa_storm_catalog


@click.group()
def cognition_disaster_data():
    pass

@cognition_disaster_data.command(name="index-noaa-storm")
@click.option('--id', type=str, multiple=True, help="ID of collection.")
@click.option('--verbose/--quiet', default=False)
def index_noaa_storm(id, verbose):
    noaa_storm_catalog(id, verbose)