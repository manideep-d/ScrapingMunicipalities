# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class ScrapingWebsitesItem(scrapy.Item):
    # define the fields for your item here like:
    links = scrapy.Field()
    text = scrapy.Field()
    municipality_name = scrapy.Field()
    valid = scrapy.Field()
    topics = scrapy.Field()
    score = scrapy.Field()
    matched_words = scrapy.Field()
    
