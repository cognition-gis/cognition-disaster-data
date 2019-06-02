import os

import scrapy
from scrapy.crawler import CrawlerProcess
import requests

from disaster_data.sources.noaa_coast.utils import get_geoinfo, get_fgdcinfo



class NoaaImageryCollections(scrapy.Spider):

    name = 'noaa-coast-imagery-collections'
    start_urls = [
        'https://coast.noaa.gov/htdata/raster2/index.html#imagery',
    ]

    @classmethod
    def crawl(cls, outfile='output.json', ids=None, items=False):
        cls.ids = ids
        cls.items = items

        process = CrawlerProcess({
            'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)',
            'FEED_FORMAT': 'json',
            'FEED_URI': outfile
        })
        process.crawl(cls)
        # Blocked while crawling
        process.start()

    def parse(self, response):
        """
        Generate a STAC Collection for each NOAA imagery project, optionally filtering by ID.
        """
        dem_table, imagery_table = response.xpath('//*[@class="sortable"]')
        imagery_head = imagery_table.xpath('.//thead//tr/th//text()').getall()

        collections = []
        collection_items = []
        ret = {}
        for row in imagery_table.xpath('.//tbody//tr'):
            values = row.xpath('.//td')
            id = values[-1].xpath('.//text()').get()
            if self.ids:
                if id not in self.ids:
                    continue

            feature = {
                "stac_version": "0.7.0",
                "properties": {},
                "assets": {},
                "extent": {}
            }

            # Converting HTML table into STAC Item
            for head, value in zip(imagery_head, values):
                links = value.xpath('.//a/@href').getall()
                data = value.xpath('.//text()').getall()

                if head == 'Dataset Name':
                    feature['assets'].update({
                        "metadata_xml": {
                            "href": links[0],
                            "type": "xml"
                        },
                        "metadata_html": {
                            "href": links[1],
                            "type": "html"
                        }
                    })
                elif head == 'https':
                    feature['assets'].update({
                        "assets_http": {
                            "href": links[0],
                            "type": "html"
                        }
                    })
                elif head == 'ftp':
                    feature['assets'].update({
                        "assets_ftp": {
                            "href": links[0],
                            "type": "ftp"
                        }
                    })
                elif head == 'DAV':
                    feature['assets'].update({
                        "asset_viewer": {
                            "href": links[0],
                            "type": "html"
                        }
                    })
                elif head == 'Tile Index':
                    feature['assets'].update({
                        "tile_index": {
                            "href": links[0],
                            "type": "shp"
                        }
                    })
                elif head == 'ID #':
                    feature.update({'id': int(data[0])})

            # Geometry handling
            geoinfo = get_geoinfo('/vsizip//vsicurl/{}/0tileindex.shp'.format(feature['assets']['tile_index']['href']))
            feature.update(geoinfo['geometry'])
            feature['extent'].update({'spatial': geoinfo['bbox']})

            # FGDC metadata
            fgdcinfo = get_fgdcinfo(feature['assets']['metadata_xml']['href'])
            feature['extent'].update({'temporal': [
                fgdcinfo['start_date'],
                fgdcinfo['end_date'],
            ]})
            feature.update({
                'title': fgdcinfo['title'],
                'description': fgdcinfo['description'],
                'processing': fgdcinfo['processing'],
            })

            collections.append(feature)

            # Scrape items
            if self.items:
                items_url = os.path.join(feature['assets']['assets_http']['href'], 'urllist{}.txt'.format(feature['id']))
                collection_items.append(self.parse_collection_items(items_url))

        ret.update({'collections': collections})
        if self.items:
            ret.update({'items': collection_items})

        return ret

    def parse_collection_items(self, file_list_url):
        r = requests.get(file_list_url)
        collection_items = r.content.decode('utf-8').splitlines()
        return ['/vsicurl/'+x for x in collection_items if x.endswith('.tif')]