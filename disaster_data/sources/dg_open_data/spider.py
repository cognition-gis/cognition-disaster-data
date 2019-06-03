import scrapy
from scrapy.crawler import CrawlerProcess

from disaster_data.sources.dg_open_data.utils import items_from_imagery_table

class DGOpenDataCollections(scrapy.Spider):
    name = 'dg-open-data'
    start_urls = [
        'https://www.digitalglobe.com/ecosystem/open-data',
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
                "properties": {
                    'legacy:date_available': date_available
                },
                "assets": {
                    "assets_http": {
                        "href": disaster_link,
                        "type": "html"
                    }
                },
                "extent": {}
            }

            yield {
                'type': 'collection',
                'data': collection
            }

            # Scrape items
            if self.items:
                yield scrapy.Request(disaster_link, callback=self.parse_disaster)



    def parse_disaster(self, response):
        event_name = response.url.split('/')[-1]

        pre_event = response.xpath('//*[@id="table--pre-event"]')
        post_event = response.xpath('//*[@id="table--post-event"]')

        pre_event_items = items_from_imagery_table(pre_event)
        post_event_items = items_from_imagery_table(post_event)

        yield {
            'type': 'item',
            'parent': event_name,
            'data': pre_event_items + post_event_items
        }