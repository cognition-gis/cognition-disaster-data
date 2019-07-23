import os

import scrapy
from scrapy.crawler import CrawlerProcess

def items_from_imagery_table(table):
    out_list = []
    for row in table.xpath('.//tbody/tr'):
        date = row.xpath('.//td/p/text()').get()
        assets = [x for x in row.xpath('.//td/a/@href').getall() if x.endswith('.tif')]
        for asset in assets:
            partial_item = {
                'type': 'Feature',
                'id': os.path.splitext(asset)[0].split('/')[-1],
                'properties': {
                    'datetime': date
                },
                'assets': {
                    'data': {
                        'href': asset
                    }
                }
            }
            out_list.append(partial_item)
    return out_list

def oam_assets_from_imagery_table(event_name, table):
    out_list = []
    for row in table.xpath('.//tbody/tr'):
        parent_id = row.xpath('.//td/ul/p/text()').get()
        assets = [x for x in row.xpath('.//td/a/@href').getall() if x.endswith('.tif') and x.split('/')[-2] == parent_id]
        oam_item = {
            "title": event_name + '_' + parent_id,
            "contact": {
                "name": "Jeff Albrecht",
                "email": "geospatialjeff@gmail.com"
            },
            "provider": "Digital Globe Open Data Program",
            "platform": "Satellite",
            "license": "CC BY-NC 4.0",
            "urls": assets
        }
        out_list.append(oam_item)
    return out_list


class DGOpenDataCatalog(scrapy.Spider):
    name = 'dg-open-data'
    start_urls = [
        'https://www.digitalglobe.com/ecosystem/open-data',
    ]
    verbose = False

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
        event_list = response.css('.event-list__event')

        for event in event_list:
            disaster_link = response.urljoin(event.xpath('.//div/a/@href').get())
            event_name = disaster_link.split('/')[-1]
            date_available = event.xpath('.//p/text()').get()

            if self.ids:
                if event_name not in self.ids:
                    continue

            collection = {
                "stac_version": "0.7.0",
                "id": event_name,
                "title": event_name,
                "description": f"Satellite imagery for {event_name}",
                "license": "CC BY-NC 4.0",
                "providers": [
                    {
                        "name": "Digital Globe",
                        "roles": ["producer", "processor", "host"],
                        "url": "http://www.digitalglobe.com/ecosystem/open-data"
                    }
                ],
                "assets": {
                    "assets_http": {
                        "href": disaster_link,
                        "type": "html"
                    }
                },
                # Backfill this once items are populated.
                # Or maybe aggregate extents inside AWS batch as STAC items are generated.
                "extent": {}
            }

            yield collection

            # Scrape items
            if self.items:
                yield scrapy.Request(disaster_link, callback=self.parse_disaster)



    def parse_disaster(self, response):
        event_name = response.url.split('/')[-1]

        pre_event = response.xpath('//*[@id="table--pre-event"]')
        post_event = response.xpath('//*[@id="table--post-event"]')

        pre_event_items = items_from_imagery_table(pre_event)
        post_event_items = items_from_imagery_table(post_event)
        all_items = pre_event_items + post_event_items

        [x.update({'collection': event_name}) for x in all_items]
        [x['properties'].update({'collection': event_name}) for x in all_items]

        # Add collection info to items
        for item in all_items:
            yield item

class DGOpenDataSummary(DGOpenDataCatalog):

    def parse(self, response):
        event_list = response.css('.event-list__event')

        for event in event_list:
            disaster_link = response.urljoin(event.xpath('.//div/a/@href').get())
            event_name = disaster_link.split('/')[-1]

            if self.ids:
                if event_name not in self.ids:
                    continue

            yield scrapy.Request(disaster_link, callback=self.parse_disaster)

    def parse_disaster(self, response):
        event_name = response.url.split('/')[-1]

        pre_event = response.xpath('//*[@id="table--pre-event"]')
        post_event = response.xpath('//*[@id="table--post-event"]')

        pre_event_items = items_from_imagery_table(pre_event)
        post_event_items = items_from_imagery_table(post_event)
        all_items = pre_event_items + post_event_items
        yield {event_name: len(all_items)}

class DGOpenDataOAM(DGOpenDataCatalog):

    def parse(self, response):
        event_list = response.css('.event-list__event')

        for event in event_list:
            disaster_link = response.urljoin(event.xpath('.//div/a/@href').get())
            event_name = disaster_link.split('/')[-1]

            if self.ids:
                if event_name not in self.ids:
                    continue

            yield scrapy.Request(disaster_link, callback=self.parse_disaster)

    def parse_disaster(self, response):
        event_name = response.url.split('/')[-1]

        pre_event = response.xpath('//*[@id="table--pre-event"]')
        post_event = response.xpath('//*[@id="table--post-event"]')

        pre_event_items = oam_assets_from_imagery_table(event_name, pre_event)
        post_event_items = oam_assets_from_imagery_table(event_name, post_event)
        all_items = pre_event_items + post_event_items

        for item in all_items:
            yield item


