import os
import subprocess
import uuid

from osgeo import gdal
import utm

from disaster_data.sources.noaa_storm import band_mappings

THUMBNAIL_BUCKET = 'cognition-disaster-data'
THUMBNAIL_KEY_PREFIX = 'thumbnails'


class Archive(object):

    def __init__(self, item, thumbdir):
        self.item = item
        self.thumbdir = thumbdir

    def download(self, out_dir):
        # subprocess.call(f"curl -OJ {self.item['archive']}")
        print("Downloading remote archive: {}".format(self.item['archive']))
        subprocess.call(f"(cd {out_dir} && curl -O {self.item['archive']})", shell=True)
        print("Finished downloading remote archive: {}".format(self.item['archive']))
        self.archive = os.path.join(out_dir, os.path.basename(self.item['archive']))
        return 1

    def listdir(self, exts=('.jpg', '.tif', '.vrt'), split_by_ext=False):
        if self.archive.endswith('.tar'):
            self.vsipath = '/vsitar/'
        elif self.archive.endswith('.zip'):
            self.vsipath = '/vsizip/'
        files =  [os.path.join(self.archive, x) for x in gdal.ReadDir(f"{self.vsipath}{self.archive}") if x.endswith(exts)]
        if split_by_ext:
            d = {}
            for ext in exts:
                d.update({ext: [x for x in files if x.endswith(ext)]})
            return d
        else:
            return files

    def spatial_resolution(self, infile, centroid):
        """Calculate spatial resolution in UTM zone"""

        utm_info = utm.from_latlon(*centroid[::-1])
        if centroid[1] > 0:
            utm_epsg = f'326{utm_info[2]}'
        else:
            utm_epsg = f'327{utm_info[2]}'
        tempfile = f"/vsimem/{uuid.uuid4()}.vrt"
        warped_vrt = gdal.Warp(tempfile, infile, dstSRS=f'EPSG:{utm_epsg}')
        spatial_res = warped_vrt.GetGeoTransform()[1]
        warped_vrt = None
        gdal.Unlink(tempfile)
        return spatial_res

    def build_thumbnail(self, item):
        thumb_splits = item['assets']['thumbnail']['href'].split('/')
        infile_splits = item['assets']['data']['href'].split('/')

        date_dir = os.path.join(self.thumbdir, thumb_splits[-2])
        if not os.path.exists(date_dir):
            os.mkdir(date_dir)

        infile = os.path.join(self.archive, infile_splits[-1])
        outfile = os.path.join(date_dir, thumb_splits[-1])
        gdal.Translate(outfile, f"{self.vsipath}{infile}", widthPct=15, heightPct=15, format='JPEG')

    def build_items(self):
        raise NotImplementedError

class ObliqueArchive(Archive):

    def __init__(self, item, thumbdir):
        super().__init__(item, thumbdir)

    def build_items(self):
        stac_items = []
        urls = self.listdir(exts=('.vrt', '.tif'), split_by_ext=True)
        for asset in urls['.vrt']:
            id = os.path.splitext(os.path.split(asset)[-1])[0]
            acq_date = asset.split('/')[-1]
            datetime = f"{acq_date[0:4]}-{acq_date[4:6]}-{acq_date[6:8]}"

            # Read spatial properties
            info = gdal.Info(f"{self.vsipath}/{asset}", format='json', allMetadata=True, extraMDDomains='all')
            geometry = info['wgs84Extent']['coordinates']
            centroid = info['cornerCoordinates']['center']
            xvals = [x[0] for x in geometry[0]]
            yvals = [y[1] for y in geometry[0]]

            partial_item = {
                'type': 'Feature',
                'id': id,
                'collection': self.item['event_name'],
                'bbox': [min(xvals), min(yvals), max(xvals), max(yvals)],
                'geometry': {
                    'type': 'Polygon',
                    'coordinates': geometry
                },
                'properties': {
                    'datetime': datetime,
                    'eo:platform': 'aerial',
                    'eo:instrument': 'TrimbleDSS',
                    'eo:bands': band_mappings.DSS,
                    'eo:gsd': self.spatial_resolution(f"{self.vsipath}/{asset}", centroid),
                    'eo:epsg': int(info['coordinateSystem']['wkt'].rsplit('"EPSG","', 1)[-1].split('"')[0])
                },
                'assets': {
                    "data": {
                        "href": os.path.join(self.item['archive'], os.path.basename(asset)),
                        "title": "Raster data",
                        "type": "application/xml",
                        "eo:bands": [
                            3, 2, 1
                        ]
                    },
                    "metadata": {
                        "href": self.item['metadata_url'],
                        "title": "FGDC metadata",
                        "type": "text/plain",
                    },
                    "thumbnail": {
                        "href": "https://{}.s3.amazonaws.com/{}".format(
                            THUMBNAIL_BUCKET,
                            os.path.join(THUMBNAIL_KEY_PREFIX, self.item['event_name'], datetime, id + '.jpg')
                        ),
                        "type": "image/jpeg",
                        "title": "Thumbnail",
                    }
                }
            }
            stac_items.append(partial_item)

            # Build thumbnail
            self.build_thumbnail(partial_item)
        return stac_items


class RGBArchive(Archive):

    def __init__(self, item, thumbdir):
        super().__init__(item, thumbdir)


    def build_items(self):
        stac_items = []
        urls = self.listdir(exts=('.tif'))
        for asset in urls:
            id = os.path.splitext(os.path.split(asset)[-1])[0]
            acq_date = asset.split('/')[-1]
            datetime = f"{acq_date[0:4]}-{acq_date[4:6]}-{acq_date[6:8]}"

            # Read spatial properties
            info = gdal.Info(f"{self.vsipath}/{asset}", format='json', allMetadata=True, extraMDDomains='all')
            geometry = info['wgs84Extent']['coordinates']
            centroid = info['cornerCoordinates']['center']
            xvals = [x[0] for x in geometry[0]]
            yvals = [y[1] for y in geometry[0]]

            partial_item = {
                'type': 'Feature',
                'id': id,
                'collection': self.item['event_name'],
                'bbox': [min(xvals), min(yvals), max(xvals), max(yvals)],
                'geometry': {
                    'type': 'Polygon',
                    'coordinates': geometry
                },
                'properties': {
                    'datetime': datetime,
                    'eo:platform': 'aerial',
                    'eo:instrument': 'TrimbleDSS',
                    'eo:bands': band_mappings.DSS,
                    'eo:gsd': self.spatial_resolution(f"{self.vsipath}/{asset}", centroid),
                    'eo:epsg': int(info['coordinateSystem']['wkt'].rsplit('"EPSG","', 1)[-1].split('"')[0])
                },
                'assets': {
                    "data": {
                        "href": os.path.join(self.item['archive'], os.path.basename(asset)),
                        "title": "Raster data",
                        "type": "image/x.geotiff",
                        "eo:bands": [
                            3, 2, 1
                        ]
                    },
                    "metadata": {
                        "href": self.item['metadata_url'],
                        "title": "FGDC metadata",
                        "type": "text/plain",
                    },
                    "thumbnail": {
                        "href": "https://{}.s3.amazonaws.com/{}".format(
                            THUMBNAIL_BUCKET,
                            os.path.join(THUMBNAIL_KEY_PREFIX, self.item['event_name'], datetime, id + '.jpg')
                        ),
                        "type": "image/jpeg",
                        "title": "Thumbnail",
                    }
                }
            }
            stac_items.append(partial_item)

            self.build_thumbnail(partial_item)

        return stac_items


class JpegTilesArchive(Archive):

    def __init__(self, item, thumbdir):
        super().__init__(item, thumbdir)

    def build_items(self):
        stac_items = []
        urls = self.listdir(exts=('.jpg', '.wld', '.jgw'), split_by_ext=True)
        urls['world_files'] = urls['.jgw'] if len(urls['.jgw']) > 0 else urls['.wld']
        for idx, asset in enumerate(urls['.jpg']):
            id = os.path.splitext(os.path.split(asset)[-1])[0]
            acq_date = asset.split('/')[-1]
            datetime = f"{acq_date[0:4]}-{acq_date[4:6]}-{acq_date[6:8]}"

            # Read spatial properties
            info = gdal.Info(f"{self.vsipath}/{asset}", format='json', allMetadata=True, extraMDDomains='all')
            geometry = info['wgs84Extent']['coordinates']
            centroid = info['cornerCoordinates']['center']
            xvals = [x[0] for x in geometry[0]]
            yvals = [y[1] for y in geometry[0]]

            partial_item = {
                'type': 'Feature',
                'id': id,
                'collection': self.item['event_name'],
                'bbox': [min(xvals), min(yvals), max(xvals), max(yvals)],
                'geometry': {
                    'type': 'Polygon',
                    'coordinates': geometry
                },
                'properties': {
                    'datetime': datetime,
                    'eo:platform': 'aerial',
                    'eo:instrument': 'TrimbleDSS',
                    'eo:bands': band_mappings.DSS,
                    'eo:gsd': self.spatial_resolution(f"{self.vsipath}/{asset}", centroid),
                    'eo:epsg': 4269
                },
                'assets': {
                    "data": {
                        "href": os.path.join(self.item['archive'], os.path.basename(asset)),
                        "title": "Raster data",
                        "type": "application/xml",
                        "eo:bands": [
                            3, 2, 1
                        ]
                    },
                    "thumbnail": {
                        "href": "https://{}.s3.amazonaws.com/{}".format(
                            THUMBNAIL_BUCKET,
                            os.path.join(THUMBNAIL_KEY_PREFIX, self.item['event_name'], datetime, id + '.jpg')
                        ),
                        "type": "image/jpeg",
                        "title": "Thumbnail",
                    },
                    "worldfile": {
                        "href": os.path.join(self.item['archive'], os.path.basename(urls['world_files'][idx])),
                        "title": "Worldfile",
                        "type": "text/plain"
                    }
                }
            }

            if len(self.item['metadata_url']) > 0:
                partial_item['assets'].update({
                    "metadata": {
                        "href": self.item['metadata_url'],
                        "title": "FGDC metadata",
                        "type": "text/plain",
                    }
                })
            stac_items.append(partial_item)

            self.build_thumbnail(partial_item)

        return stac_items