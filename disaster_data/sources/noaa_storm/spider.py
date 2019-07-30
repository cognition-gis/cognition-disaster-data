import os
from urllib.parse import urljoin

import scrapy
from scrapy.crawler import CrawlerProcess

class NoaaStormCatalog(scrapy.Spider):
    name = 'noaa-storm'
    start_urls = [
        "https://storms.ngs.noaa.gov/"
    ]
    verbose = True

    @classmethod
    def crawl(cls, outfile='output.json', ids=None, items=False):
        cls.ids = ids
        cls.items = items

        opts = {
            'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)',
            'FEED_FORMAT': 'json',
            'FEED_URI': outfile,
        }


        if not cls.verbose:
            opts.update({'LOG_ENABLED': False})

        process = CrawlerProcess(opts)
        process.crawl(cls)
        # Blocked while crawling
        process.start()

    def parse(self, response):
        """
        Base scraper which scrapes each disaster link
        """
        event_list = response.xpath("//div[contains(@class,'layout_col1')]/h2/a")
        for event in event_list:
            event_name = event.xpath('text()').get().split('(')[0][:-1].replace(',', '').replace('.', '').replace(' ', '-').lower()
            event_link = event.xpath('@href').get()

            if self.ids:
                if event_name not in self.ids:
                    continue

            collection = {
                "id": event_name + '@storm',
                "title": event_name + '@storm',
                "description": "",
                "stac_version": "0.7.0",
                "license": "U.S. Government Work",
                "providers": [
                    {
                        "name": "NOAA NGS",
                        "roles": ["producer", "processor", "host"],
                        "url": "https://www.ngs.noaa.gov/"
                    }
                ],
                "assets": {
                    "NOAA Coast viewer": {
                        "href": event_link,
                        "type": "text/html",
                        "title": "Online data viewer"

                    }
                },
                "links": [
                    {
                        "href": "https://www.usa.gov/government-works",
                        "type": "text/html",
                        "rel": "license"
                    }
                ]
            }

            yield collection

            yield scrapy.Request(event_link, callback=self.parse_disaster, meta={'event_name': event_name + '@storm'})

    def parse_disaster(self, response):
        # There are two different viewer formats used by NOAA Storm
        # Check the header to determine the format
        format_check = response.xpath("//head/meta[@name='viewport']")

        if 'geodesy.noaa.gov' in response.url:
            event_id = response.url.split('/')[-2]
            if '_' in event_id:
                event_id = event_id.split('_')[-1]
        else:
            event_id = response.url.split('/')[-2]

        # If viewport is present, page is using modern format
        # Each item yielded by Scrapy links to a TAR file containing many images and (sometimes) an index shapefile
        if len(format_check) > 0:

            download_links = [
                x.get() for x in response.xpath("//ul[contains(@class,'dropdown-menu')]/li/a/@href") if x.get().endswith('_RGB.tar')
            ]

            tile_index_url = [f'/vsitar//vsicurl/{x}/{x.split("/")[-1].split("_")[0]}_tile_index.shp' for x in download_links]
            metadata_url = response.xpath("//div[@id='metadata']/ul/li/a/@href").get()

            for idx, link in enumerate(download_links):
                yield {
                    'type': 'modern',
                    'event_name': response.meta['event_name'],
                    'archive': link,
                    'tile_index': tile_index_url[idx],
                    'metadata_url': metadata_url
                }
        # If viewport is not present, page is using old format
        # Each item yielded by Scrapy links to a single JPG file with world file (JGW).
        else:
            # Find the index
            index = [
                x.get() for x in response.xpath("//td[contains(@class,'normaltext')]/a/@href") if event_id in x.get().lower() and
                                                                                                  'https' not in x.get()
            ][0]
            yield scrapy.Request(os.path.join(os.path.dirname(response.url), index), callback=self.parse_map_index, meta=response.meta)

    def parse_map_index(self, response):
        map = response.xpath("//map/div/area/@href")
        for url in map:
            yield scrapy.Request(os.path.join(os.path.dirname(response.url), url.get()), callback=self.parse_image_index, meta=response.meta)

    def parse_image_index(self, response):
        map = response.xpath("//map/div/area/@href")
        for item in map:
            url = item.get()
            if url.endswith('.htm'):
                yield scrapy.Request(os.path.join(os.path.dirname(response.url), url), callback=self.parse_image_page, meta=response.meta)
            else:
                # There is no image page, just a JPG.  No world file either.
                # Could potentially rebuild a world file but ignoring for now.
                pass

    def parse_image_page(self, response):
        payload = {
            'type': 'old',
            'event_name': response.meta['event_name']
        }

        col1, col2, col3 = response.xpath("//td")

        # Parse datetime information from the page
        date, time = col3.xpath('//font/text()[preceding-sibling::br and following-sibling::br]').getall()[1:3]
        splits = date.replace('\n', '').replace(' ', '').replace('/', '-').split(':')[-1].split('-')
        if int(splits[0]) < 10:
            splits[0] = f"0{splits[0]}"
        date = "-".join([splits[-1], splits[0], splits[1]])
        time = ':'.join(time.replace('\n', '').replace(' ', '').split(':')[1:])
        payload.update({
            'datetime': f"{date}T{time}.00Z"
        })

        rows = col1.xpath('//a')
        for row in rows:
            rel = row.xpath('text()').get()
            link = row.xpath('@href').get()

            if rel == 'Full Size Image':
                payload.update({'urls': [urljoin(response.url, link)]})
            elif rel == 'World File':
                payload.update({'world_file': urljoin(response.url, link)})
            elif rel == 'Metadata File':
                payload.update({'metadata_url': urljoin(response.url, link[1:])})

        # Only return the image if it has a world file
        if 'world_file' in payload:
            yield payload