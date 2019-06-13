import os
import tempfile
import shutil
import json


class ScrapyRunner(object):

    """Run a scrapy spider (with context)"""

    @staticmethod
    def create_tempdir():
        tempdir = tempfile.mkdtemp()
        return tempdir

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleanup()

    def __init__(self, spider):
        self.spider = spider
        self.tempdir = self.create_tempdir()

    def cleanup(self):
        shutil.rmtree(self.tempdir)

    def execute(self, **kwargs):
        tempdir = tempfile.mkdtemp()
        outfile = os.path.join(tempdir, 'output.json')
        self.spider.crawl(outfile=outfile, **kwargs)

        with open(outfile, 'r') as geoj:
            data = json.load(geoj)

            # Sort out the collections
            collections = [data.pop(data.index(x)) for x in data if 'collection' not in x]

            # Make sure first item of generator are collections
            yield collections

            for item in data:
                yield item

