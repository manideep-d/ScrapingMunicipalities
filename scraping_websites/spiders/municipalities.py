from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from ..items import ScrapingWebsitesItem

from ..utils import parsing_doc_ext_as_doc_or_docx,parsing_doc_ext_as_pdf,parsing_doc_ext_as_html

#to get domain name
import tldextract

import logging

logger = logging.getLogger(__name__).setLevel(logging.ERROR)


TEXTRACT_EXTENSIONS = [".doc", ".docx",".pdf"]

class CustomLinkExtractor(LinkExtractor):
    """ Custom Link Extractor with scraping from allowed extension """
    def __init__(self, *args, **kwargs):
        super(CustomLinkExtractor, self).__init__(*args, **kwargs)
        # Keep the default values in "deny_extensions" *except* for those types we want.
        self.deny_extensions = [ext for ext in self.deny_extensions if ext not in TEXTRACT_EXTENSIONS]


class MunicipalitiesSpider(CrawlSpider):
    """ Spider which crawls the websites of municipalities """

    name = 'MunicipalitiesSpider'
   
    allowed_domains =['richmondhill.ca','niagarafalls.ca','niagarafalls.civicweb.net','vaughan.ca','winnipeg.ca','mississauga.ca']
   

    start_urls=['https://www.richmondhill.ca','https://niagarafalls.ca/','https://niagarafalls.civicweb.net/portal/',
    'https://www.winnipeg.ca/interhom/',"https://www.mississauga.ca/"]
   
    word_list=["img","facebook","twitter","youtube","instagram","maps","map","zoom","webex","linkedin","you","story","calendar","cem","google","form","survey","meetings","archives"]

    rules = (
    Rule(CustomLinkExtractor(deny=word_list), callback='parse_item', follow=True),
    )

    def parse_item(self, response):
        """ Parsing content recieved from the each link. And it yields the items dictionary """

        items = ScrapingWebsitesItem()

        url = response.url

        ext = tldextract.extract(url)

        items['municipality_name'] = ext.domain
        items['links'] = url
        items['valid'] = False

        extension = list(filter(lambda x: response.url.lower().endswith(x), TEXTRACT_EXTENSIONS))

        if  response.url.lower().endswith('doc') or  response.url.lower().endswith('docx'):
           items = parsing_doc_ext_as_doc_or_docx(extension,response,items)

        elif(response.url.lower().endswith('pdf')):
           items = parsing_doc_ext_as_pdf(url,response,items)

        else:
           items = parsing_doc_ext_as_html(response,items)

        yield items

