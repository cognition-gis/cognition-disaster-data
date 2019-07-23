import json
import os

from osgeo import gdal
import utm

def build_base_item(item, url):
    id = os.path.splitext(os.path.split(url)[-1])[0]
    acq_date = url.split('/')[-2].split('_')[0]

    partial_item = {
        'type': 'Feature',
        'id': id,
        'properties': {
            'datetime': f"{acq_date[0:4]}-{acq_date[4:6]}-{acq_date[6:8]}",
        },
        'assets': {
            "data": {
                "href": url,
                "title": "Raster data",
                "type": "image/x.geotiff"
            },
            "metadata": {
                "href": item['metadata_url'],
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
        item['properties'].update({
            'eo:gsd': warped_vrt.GetGeoTransform()[1]
        })
    finally:
        warped_vrt = None
        gdal.Unlink(f'/vsimem/{item["id"]}.vrt')

    return item


def remote_listdir(dir):
    return [os.path.join(dir, x) for x in gdal.ReadDir(f"/vsitar//vsicurl/{dir}")]

def build_stac_catalog():

    with open('output.json', 'r') as f:
        scraped_items = json.load(f)
        for item in scraped_items:

            if item['type'] == 'modern':
                urls = remote_listdir(item['download_link'])
                for url in urls:
                    kwargs = {
                        'item': item,
                        'url': url
                    }
                    base_item = build_base_item(**kwargs)
                    append_gdal_info(base_item)

build_stac_catalog()