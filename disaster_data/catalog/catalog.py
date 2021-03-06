import os
import json

from satstac import Catalog, Collection
from disaster_data.utils import gdal_info_stac_multi

# Root catalog definition
cat_json = {
    "id": "cognition-disaster-data",
    "stac_version": "0.7.0",
    "description": "Geospatial disaster data."
}

# Datasource specific catalogs
datasource_catalogs = {
    "NOAA": {
        "id": "NOAACoast",
        "stac_version": "0.7.0",
        "description": "Disaster data scraped from NOAA Coast FTP server (https://coast.noaa.gov/htdata/raster2/index.html#imagery)"
    },
    "DigitalGlobe": {
        "id": "DGOpenData",
        "stac_version": "0.7.0",
        "description": "DG imagery scraped from Digital Globe Open Data Program (https://www.digitalglobe.com/ecosystem/open-data)"
    }
}


def organize_stac_assets(assets):
    # Find all collections and items
    collections = [x['data'] for x in assets if x['type'] == 'collection']
    items = [x for x in assets if x['type'] == 'item']

    organized_collections = {}
    for coll in collections:
        organized_collections.update({coll['id']: coll})

    organized_items = {}
    for item in items:
        organized_items.update({item['parent']: item['data']})

    organized_stac = {
        'collections': organized_collections,
        'items': organized_items
    }


    return organized_stac

class DisasterDataCatalog(object):

    def __init__(self, root):
        self.root = root

    def create_root_catalog(self):
        cat = Catalog(cat_json, root=self.root)
        cat.save_as(os.path.join(self.root, 'catalog.json'))

    def create_datasource_catalog(self, ds_name):
        cat = Catalog.open(os.path.join(self.root, 'catalog.json'))
        ds_cat = Catalog(datasource_catalogs[ds_name])
        cat.add_catalog(ds_cat)
        cat.save()

    def create_year_catalogs(self, year_list, ds_name):
        for year in year_list:
            year_catalog = Catalog({
                "id": year,
                "stac_version": "0.7.0",
                "description": "Data acquired during the year {}".format(year)
            })
            cat = Catalog.open(os.path.join(self.root, ds_name, 'catalog.json'))
            cat.add_catalog(year_catalog)
            cat.save()

    def create_project_collections(self, projects, ds_name):
        with open(projects, 'r') as geoj:
            data = json.load(geoj)
            for feat in data['features']:
                if feat['extent']['temporal'][0]:
                    year = feat['extent']['temporal'][0].split('-')[0]
                    year_cat = Catalog.open(os.path.join(self.root, ds_name, year, 'catalog.json'))
                    coll = Collection(feat)
                    year_cat.add_catalog(coll)
                    year_cat.save()