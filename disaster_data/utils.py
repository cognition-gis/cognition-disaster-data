import os
from multiprocessing.pool import ThreadPool

from osgeo import gdal


def gdal_info_stac(input_item):
    """
    Return incomplete STAC item from call to gdal.Info
    """
    file_url = os.path.join("/vsicurl/" + input_item['item']['assets']['data']['href'])
    info = gdal.Info(file_url, format='json', allMetadata=True)

    # Calculating geometry and bbox
    geometry = info['wgs84Extent']['coordinates']
    xvals = [x[0] for x in geometry[0]]
    yvals = [y[1] for y in geometry[0]]


    # Strip vsi reference if present
    filename = info['files'][0]
    if filename.startswith('/vsi'):
        filename = '/'.join(filename.split('/')[2:])

    partial_item = {
        'id': os.path.splitext(os.path.split(filename)[-1])[0],
        'bbox': [min(xvals), min(yvals), max(xvals), max(yvals)],
        'geometry': {
            'type': 'Polygon',
            'coordinates': geometry
        },
        'properties': {
            'eo:epsg': info['coordinateSystem']['wkt'].rsplit('"EPSG","', 1)[-1].split('"')[0],
            'eo:gsd': (info['geoTransform'][1] + abs(info['geoTransform'][-1])) / 2,
        },
        # 'assets': {
        #     'data': {
        #         'href': filename
        #     }
        # }
    }

    # Merge the input STAC item (generated from Scrapy) with partial STAC item (from gdal.Info)
    merged = {**partial_item, **input_item['item']}
    merged['properties'].update(partial_item['properties'])

    return {'parent': input_item['parent'], 'item': merged}

def gdal_info_stac_multi(filelist, num_threads=10):
    m = ThreadPool(num_threads)
    response = m.map(gdal_info_stac, filelist)
    m.close()
    m.join()
    return response