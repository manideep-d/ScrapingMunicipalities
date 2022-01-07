# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import pymongo
from scrapy.exceptions import DropItem
#from scrapy import log

class ScrapingWebsitesPipeline:

    def __init__(self):
        self.conn = pymongo.MongoClient('localhost',27017)
        db = self.conn['scarped_documents']
        self.collection = db['with_topics']
        self.links_retrieved = set()

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        if item['links'] in self.links_retrieved:
            raise DropItem(f"Duplicate item found: {item!r}")
        else:
            #https://stackoverflow.com/questions/53006922/dropping-duplicate-items-from-scrapy-pipeline
            #https://doc.scrapy.org/en/latest/topics/item-pipeline.html#duplicates-filter
            self.links_retrieved.add(item['links'])
            if item['valid'] :
                self.collection.insert(dict(item))
                return item
