from osgeo import gdal

def gdal_info_stac(infile):
    """
    Return incomplete STAC item from call to gdal.Info
    """

    info = gdal.Info(infile, format='json', allMetadata=True)

    # Calculating geometry and bbox
    geometry = info['wgs84Extent']['coordinates']
    xvals = [x[0] for x in geometry[0]]
    yvals = [y[1] for y in geometry[0]]


    # Strip vsi reference if present
    filename = info['files'][0]
    if filename.startswith('/vsi'):
        filename = '/'.join(filename.split('/')[2:])

    partial_item = {
        'bbox': [min(xvals), min(yvals), max(xvals), max(yvals)],
        'geometry': {
            'type': 'Polygon',
            'coordinates': geometry
        },
        'properties': {
            'eo:epsg': info['coordinateSystem']['wkt'].rsplit('"EPSG","', 1)[-1].split('"')[0],
            'eo:gsd': (info['geoTransform'][1] + abs(info['geoTransform'][-1])) / 2,
        },
        'asset': {
            'data': {
                'href': filename
            }
        }
    }

    return partial_item