import json
import os

from osgeo import gdal
import utm



def build_stac_catalog():

    with open('output.json', 'r') as f:
        scraped_items = json.load(f)

        for item in scraped_items:
            if item['type'] == 'modern':

                # Use GDAL to read directory
                filelist = [os.path.join(item['download_link'], x) for x in gdal.ReadDir(f"/vsitar//vsicurl/{item['download_link']}")]

                for file in filelist:
                    info = gdal.Info(f"/vsitar//vsicurl/{file}", format='json', allMetadata=True)

                    id = os.path.splitext(os.path.split(file)[-1])[0]
                    acq_date = file.split('/')[-2].split('_')[0]
                    geometry = info['wgs84Extent']['coordinates']
                    centroid = info['cornerCoordinates']['center']
                    xvals = [x[0] for x in geometry[0]]
                    yvals = [y[1] for y in geometry[0]]

                    # Reproject to appropriate UTM zone via VRT to read spatial resolution in meters.
                    utm_info = utm.from_latlon(*centroid[::-1])
                    if centroid[1] > 0:
                        utm_epsg = f'326{utm_info[2]}'
                    else:
                        utm_epsg = f'327{utm_info[2]}'
                    warped_vrt = gdal.Warp(f'/vsimem/{id}.vrt', f"/vsitar//vsicurl/{file}", dstSRS=f'EPSG:{utm_epsg}')

                    partial_item = {
                        'type': 'Feature',
                        'id': os.path.splitext(os.path.split(file)[-1])[0],
                        'bbox': [min(xvals), min(yvals), max(xvals), max(yvals)],
                        'properties': {
                            'datetime': f"{acq_date[0:4]}-{acq_date[4:6]}-{acq_date[6:8]}",
                            'eo:gsd': warped_vrt.GetGeoTransform()[1]
                        },
                        'geometry': {
                            'type': 'Polygon',
                            'coordinates': geometry
                        },
                        'assets': {
                            "data": {
                                "href": file,
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

                    print(partial_item)

                    # Delete memory file
                    gdal.Unlink(f'/vsimem/{id}.vrt')

build_stac_catalog()