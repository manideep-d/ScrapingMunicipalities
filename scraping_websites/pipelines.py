# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import pymongo
from scrapy.exceptions import DropItem

import os

class ScrapingWebsitesPipeline:

    def __init__(self):
        """ Initializing the db proeprties """
        
        HOST_NAME = os.environ['HOST_NAME']
        PORT = os.environ['PORT']
        DATABASE_NAME = os.environ['DATABASE_NAME']
        COLLECTION_NAME = os.environ['COLLECTION_NAME']

        self.conn = pymongo.MongoClient(HOST_NAME,PORT)
        db = self.conn[DATABASE_NAME]
        self.collection = db[COLLECTION_NAME]

        self.links_retrieved = set()

    def process_item(self, item, spider):
        """ Processing each item and saving it to the db and it won't add duplicate link to database """

        adapter = ItemAdapter(item)

        if adapter['links'] in self.links_retrieved:
            raise DropItem(f"Duplicate item found: {item!r}")
        else:
            #https://stackoverflow.com/questions/53006922/dropping-duplicate-items-from-scrapy-pipeline
            #https://doc.scrapy.org/en/latest/topics/item-pipeline.html#duplicates-filter
            self.links_retrieved.add(item['links'])
            if item['valid'] :
                self.collection.insert(dict(item))
                return item