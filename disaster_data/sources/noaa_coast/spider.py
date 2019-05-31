import scrapy

from scrapy.crawler import CrawlerProcess

from disaster_data.sources.noaa_coast.utils import get_geoinfo, get_fgdcinfo

class NoaaImageryCollections(scrapy.Spider):
    name = 'noaa-coast'
    start_urls = [
        'https://coast.noaa.gov/htdata/raster2/index.html#imagery',
    ]

    @classmethod
    def crawl(cls, outfile='output.json'):
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
        Base scraper which generates a STAC Collection for each NOAA imagery project.
        """
        dem_table, imagery_table = response.xpath('//*[@class="sortable"]')
        imagery_head = imagery_table.xpath('.//thead//tr/th//text()').getall()

        collections = []
        for row in imagery_table.xpath('.//tbody//tr'):
            values = row.xpath('.//td')

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

        return {
            "type": "FeatureCollection",
            "features": collections
        }
