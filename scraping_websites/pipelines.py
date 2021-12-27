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
        self.collection = db['scarped_documents']

    def process_item(self, item, spider):
        if item['valid'] :
            self.collection.insert(dict(item))
            #log.msg("Added to MongoDB database!",level=log.DEBUG, spider=spider)
        return item
