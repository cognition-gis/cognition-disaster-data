import json

from osgeo import ogr
from shapely.ops import cascaded_union
from shapely.geometry import Polygon
import geojson
import requests
import xml.etree.ElementTree as ET
from gis_metadata.metadata_parser import get_metadata_parser

gmd_tag = '{http://www.isotc211.org/2005/gmd}'
gml_tag = '{http://www.opengis.net/gml/3.2}'

def get_geoinfo(fpath):
    """
    Returns the exact extent of all geometries within a vector.
    """
    ds = ogr.Open(fpath)
    lyr = ds.GetLayer()

    poly_list = []
    for feat in lyr:
        coords = json.loads(feat.ExportToJson())['geometry']['coordinates'][0]
        poly = Polygon(coords)
        # Handle for self-intersections
        buffered = poly.buffer(0.000001)
        poly_list.append(buffered)

    dissolved = cascaded_union(poly_list)
    geoj = getattr(geojson, dissolved.geom_type)(geometry=dissolved)
    geo_info = {
        "geometry": json.loads(geojson.dumps(geoj)),
        "bbox": list(dissolved.bounds)
    }
    del geo_info['geometry']['coordinates']

    return geo_info

def get_fgdcinfo(fpath):
    """
    Gathers information from FGDC metadata attached to each NOAA project.
    """
    r = requests.get(fpath)

    md_parser = get_metadata_parser(r.content)
    root = ET.fromstring(r.content)

    start_date = root.findall(f'.//{gml_tag}beginPosition')
    end_date = root.findall(f'.//{gml_tag}endPosition')

    if len(start_date) == len(end_date) == 0:
        start_date = end_date = root.findall(f'.//{gml_tag}timePosition')

    if len(start_date) == len(end_date) == 0:
        start_date = end_date = None
    else:
        start_date = start_date[0].text
        end_date = end_date[0].text

    return {
        'title': md_parser.title,
        'description': md_parser.abstract,
        'processing': md_parser.process_steps,
        'start_date': start_date,
        'end_date': end_date,
    }