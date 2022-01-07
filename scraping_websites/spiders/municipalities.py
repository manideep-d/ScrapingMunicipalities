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
import io
# For getting information about the pdfs
from PyPDF2 import PdfFileReader
#imports for LDA model
from itertools import chain
import requests
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
    #allowed_domains = ['niagarafalls.civicweb.net']
    allowed_domains =['citywindsor.ca']
    #allowed_domains =['richmondhill.ca']
    #allowed_domains =['vaughan.ca']
    #allowed_domains =['bair.berkeley.edu']

    #start_urls = ['https://niagarafalls.civicweb.net/portal/']  
    start_urls=['https://citywindsor.ca']
    #start_urls = ['https://www.vaughan.ca/Pages/Home.aspx']
    #start_urls = ['https://bair.berkeley.edu/blog/']
    #start_urls=['https://www.richmondhill.ca']
    #start_urls =['https://www.richmondhill.ca/Modules/News/index.aspx?feedId=5988c08a-c0f5-4d51-91e0-9691f68738f4,b178fbf3-ca63-4d70-b2ed-ef140381b794,05eeed24-434e-4a9c-996a-4147a96024ec&keyword=modernize&newsId=174a5617-ce94-4ed7-a851-43eba029c125']
    word_list=["img","facebook","twitter","youtube","instagram","maps","map","zoom","webex","linkedin","you","story","calendar","cem","google","form","survey"]

    rules = (
    Rule(CustomLinkExtractor(deny=word_list), callback='parse_item', follow=True),
    )

    def parse_item(self, response):
        items = ScrapingWebsitesItem()

        url = response.url

        if 'www' in url:
            items['municipality_name'] = url.split(".")[1]

        else:
            items['municipality_name'] = url.split("//")[1].split(".")[0]

        
        items['links'] = url
        
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

                    if (self.finding_the_topics(extracted_data,items) == True):
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
                    response = requests.get(url)
                    with fitz.open(stream=response.content, filetype="pdf") as doc:
                        text = ''
                        for page in doc:
                            text += page.getText()

                    #response = requests.get(url)

                    #from geeksforgeeks
                    #with io.BytesIO(response.content) as f:
                        #pdf = PdfFileReader(f)
                        #information = pdf.getDocumentInfo()
                        #number_of_pages = pdf.getNumPages()
                
                    #text = f
                    
                    if (self.finding_the_topics(text,items) == True):

                        items['text'] = text
                        items['valid'] = True
                        #with open(path,'w') as file:
                            #file.write(text)
                            #file.close()
                    else:
                            pass

                except (Exception,RuntimeError) as e: 
                        print(e)
                        raise
                        #pass


        else:

                try:
                        html = response.body
                        soup = BeautifulSoup(html,"html.parser")
                        text =  soup.get_text()

                        now = str(datetime.now())
                        title = response.url.split("/")[-2]

                        path = '/Users/manideep/Documents/DirectedStudy/new/'+str(title)+now+'.txt'

                        if (self.finding_the_topics(text,items)== True):

                            items['text'] = text
                            items['valid'] = True

                            #with open(path,'w') as file:
                                #file.write(text)
                                #file.close()
                        else:
                            pass
                except Exception as e: 
                        print(e)
                        raise
                        #pass

        yield items


    def finding_the_topics(self,text,items):

        model = self.lda_Model(text)

        topics= model.show_topics(formatted=True, num_topics=15, num_words=20)
        topics_strng = ''.join(str(e) for e in topics)
        items['topics'] = topics_strng

        words = ["artificialintelligence","artificial","intelligence","smart","autonoums","ai","informationtechnology",
        "smartcities","intelligent","sensors","smartest","gps","smartparking","businessintelligence","dataanalytics","machinelearning",
        "deeplearning","computervision","nlp","analytics","machine","learning","analytics","technology","rover","computer"]

        score =0
        macthed_words = []

       # https://www.machinelearningplus.com/nlp/topic-modeling-gensim-python/#10removestopwordsmakebigramsandlemmatize
        for i in range(15):
            wp = model.show_topic(i, topn=20)
            topic_keywords = ", ".join([word.replace("_",",") for word, prop in wp])
            topic_keywords = topic_keywords.split(",")
           
            #topic_keywords = [word for word in wp]
            for word in topic_keywords:
                for string in words:
                    if(string == word):
                        macthed_words.append(word)
                        score = score + 1

        if score > 1:
            items['score'] = score
            macthedwords = ' '.join(map(str, macthed_words))
            items['matched_words'] = macthedwords
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
        

        stopwords_verbs = ['say', 'get', 'go', 'know', 'may', 'need', 'like', 'make', 'see', 'want', 'come', 'take', 'use', 'would', 'can','and']
        stopwords_other = ['one', 'image', 'getty', 'de', 'en', 'caption', 'also', 'copyright', 'something','browser','contact','us','telephone','email','phone','home',
        'sitemap','map','like','tweet','subscribe','alerts','share','things','terms','register','apply']
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
        num_topics = 15

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

