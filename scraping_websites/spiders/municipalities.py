import scrapy
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from ..items import ScrapingWebsitesItem
from datetime import datetime
import fitz
from bs4 import BeautifulSoup
from tempfile import NamedTemporaryFile
import re
import textract
#imports for LDA model
from itertools import chain
import os
import pandas as pd
import numpy as np
from tqdm import tqdm_notebook
tqdm_notebook().pandas()
from nltk.tokenize import sent_tokenize
from nltk.tokenize import word_tokenize
from nltk import pos_tag
from nltk.corpus import wordnet
from nltk.stem.wordnet import WordNetLemmatizer
from nltk.corpus import stopwords
from gensim.models import Phrases
from gensim import corpora
from gensim import models



#https://www.imagescape.com/blog/2018/08/20/scraping-pdf-doc-and-docx-scrapy/
control_chars = ''.join(map(chr, chain(range(0, 9), range(11, 32), range(127, 160))))
CONTROL_CHAR_RE = re.compile('[%s]' % re.escape(control_chars))
TEXTRACT_EXTENSIONS = [".doc", ".docx",".pdf"]

class CustomLinkExtractor(LinkExtractor):
    def __init__(self, *args, **kwargs):
        super(CustomLinkExtractor, self).__init__(*args, **kwargs)
        # Keep the default values in "deny_extensions" *except* for those types we want.
        self.deny_extensions = [ext for ext in self.deny_extensions if ext not in TEXTRACT_EXTENSIONS]


class MunicipalitiesSpider(CrawlSpider):
    name = 'MunicipalitiesSpider'
    #allowed_domains = ['niagarafalls.ca','niagarafalls.civicweb.net']
    allowed_domains = ['niagarafalls.civicweb.net']
    #allowed_domains =['citywindsor.ca']
    #allowed_domains =['richmondhill.ca']
    #allowed_domains =['vaughan.ca']
    #allowed_domains =['bair.berkeley.edu']

    start_urls = ['https://niagarafalls.civicweb.net/portal/']  
    #start_urls=['https://citywindsor.ca']
    #start_urls = ['https://www.vaughan.ca/Pages/Home.aspx']
    #start_urls = ['https://bair.berkeley.edu/blog/']
    #start_urls=['https://www.richmondhill.ca']
    word_list=["img","facebook","twitter","youtube","instagram","maps","map","zoom","webex","linkedin","you","story","calendar","cem","google","form","survey"]

    rules = (
    Rule(CustomLinkExtractor(deny=word_list), callback='parse_item', follow=True),
    )

    def parse_item(self, response):
        items = ScrapingWebsitesItem()

        url = response.url
        
        items['links'] = url
        items['municipality_name'] = url.split(".")[1]
        items['valid'] = False

        extension = list(filter(lambda x: response.url.lower().endswith(x), TEXTRACT_EXTENSIONS))

        if  response.url.lower().endswith('doc') or  response.url.lower().endswith('docx'):

                title = url.split("/")[-1].split(".")[0]
                now = str(datetime.now())

                path = '/Users/manideep/Documents/DirectedStudy/new/'+str(title)+now+'.txt'
    
                try:

                    tempfile = NamedTemporaryFile(suffix=str(extension))
                    tempfile.write(response.body)
                    tempfile.flush()
                    extracted_data = textract.process(tempfile.name)
                    extracted_data = extracted_data.decode('utf-8')
                    extracted_data = CONTROL_CHAR_RE.sub('', extracted_data)
                    tempfile.close()

                    if (self.finding_the_topics(extracted_data) == True):
                        #with open(path,'w') as file:
                            #file.write(extracted_data)
                            #file.close()
                        items['text'] = extracted_data
                        items['valid'] = True

                    else:
                            pass

                except (Exception,RuntimeError) as e: 
                        print(e)
                        raise
                        #pass
        elif(response.url.lower().endswith('pdf')):

                title = url.split("/")[-1].split(".pdf")[0]
                now = str(datetime.now())

                path = '/Users/manideep/Documents/DirectedStudy/new/'+str(title)+now+'.txt'

                try:


                    with fitz.open(url) as doc:
                        text = ''
                        for page in doc:
                            text += page.getText()
                    
                    if (self.finding_the_topics(text) == True):

                        items['text'] = text
                        items['valid'] = True
                        #with open(path,'w') as file:
                            #file.write(text)
                            #file.close()
                    else:
                            pass

                except (Exception,RuntimeError) as e: 
                        print(e)
                        #raise
                        pass


        else:

                try:
                        html = response.body
                        soup = BeautifulSoup(html,"html.parser")
                        text =  soup.get_text()

                        now = str(datetime.now())
                        title = response.url.split("/")[-2]

                        path = '/Users/manideep/Documents/DirectedStudy/new/'+str(title)+now+'.txt'

                        if (self.finding_the_topics(text)== True):

                            items['text'] = text
                            items['valid'] = True

                            #with open(path,'w') as file:
                                #file.write(text)
                                #file.close()
                        else:
                            pass
                except Exception as e: 
                        print(e)
                        #raise
                        pass

        yield items


    def finding_the_topics(self,text):

        model = self.lda_Model(text)

        words = ["artificial","intelligence","artificialintelligence","smart","autonoums","ai","it","informationtechnology","smartcities","intelligent","sensor","smartest","gps","streetlighting","smartparking","businessintelligence","dataanalytics"]

        for i in range(15):
            wp = model.show_topic(i, topn=20)
            topic_keywords = ", ".join([word.replace("_",",") for word, prop in wp])
            topic_keywords = topic_keywords.split(",")
           
            #topic_keywords = [word for word in wp]
            for word in topic_keywords:
                for string in words:
                    if(string == word):
                        return True


    def lda_Model(self,text):

        sentences = sent_tokenize(text)

        tokens_sentences =  [word_tokenize(sentence) for sentence in sentences]

        POS_tokens = [pos_tag(tokens) for tokens in tokens_sentences]

        lemmatizer = WordNetLemmatizer()

        tokens_sentences_lemmatized = [
        [
            lemmatizer.lemmatize(el[0], self.get_wordnet_pos(el[1])) 
            if self.get_wordnet_pos(el[1]) != '' else el[0] for el in tokens_POS
        ] 
        for tokens_POS in POS_tokens
        ]
        

        stopwords_verbs = ['say', 'get', 'go', 'know', 'may', 'need', 'like', 'make', 'see', 'want', 'come', 'take', 'use', 'would', 'can']
        stopwords_other = ['one', 'image', 'getty', 'de', 'en', 'caption', 'also', 'copyright', 'something','browser','contact','us']
        my_stopwords = stopwords.words('English') + stopwords_verbs + stopwords_other

        #tokens = list(map(lambda sentences: list(chain.from_iterable(sentences),tokens_sentences_lemmatized)))
        tokens = list(map(lambda tokens: [token.lower() for token in tokens if token.isalpha() 
                                                    and token.lower() not in my_stopwords and len(token)>=1],tokens_sentences_lemmatized))

        #tokens = data['tokens'].tolist()
        bigram_model = Phrases(tokens)
        trigram_model = Phrases(bigram_model[tokens], min_count=1)
        tokens = list(trigram_model[bigram_model[tokens]])                                        

        dictionary_LDA = corpora.Dictionary(tokens)

        #dictionary_LDA.filter_extremes(no_below=3)
        corpus = [dictionary_LDA.doc2bow(tok) for tok in tokens]

        np.random.seed(45)
        num_topics = 20

        lda_model = models.LdaMulticore(corpus, num_topics=num_topics, \
                                  id2word=dictionary_LDA, \
                                  passes=10, alpha=[0.01]*num_topics, \
                                  eta=[0.01]*len(dictionary_LDA.keys()))
        
        return lda_model

    

    def get_wordnet_pos(self,treebank_tag):

        if treebank_tag.startswith('J'):
            return wordnet.ADJ
        elif treebank_tag.startswith('V'):
            return wordnet.VERB
        elif treebank_tag.startswith('N'):
            return wordnet.NOUN
        elif treebank_tag.startswith('R'):
            return wordnet.ADV
        else:
            return ''

