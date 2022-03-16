import fitz
from bs4 import BeautifulSoup
from tempfile import NamedTemporaryFile
import re
import textract

#imports for LDA model
from itertools import chain
import requests

import nltk
nltk.download('omw-1.4')

from nltk.tokenize import sent_tokenize
from nltk.tokenize import word_tokenize
from nltk import pos_tag
from nltk.corpus import wordnet
from nltk.stem.wordnet import WordNetLemmatizer
from nltk.corpus import stopwords
from gensim.models import Phrases
from gensim import corpora
from gensim import models



num_topics = 14  #This is optimal number of topics got from the model trained on whole corpus of two municipalities sites.

#https://www.imagescape.com/blog/2018/08/20/scraping-pdf-doc-and-docx-scrapy/
control_chars = ''.join(map(chr, chain(range(0, 9), range(11, 32), range(127, 160))))
CONTROL_CHAR_RE = re.compile('[%s]' % re.escape(control_chars))


def parsing_doc_ext_as_doc_or_docx(extension,response,items):
    """ Parses the text if the extension is doc or docx """
    try:
        tempfile = NamedTemporaryFile(suffix=str(extension))
        tempfile.write(response.body)
        tempfile.flush()
        extracted_data = textract.process(tempfile.name)
        extracted_data = extracted_data.decode('utf-8')
        extracted_data = CONTROL_CHAR_RE.sub('', extracted_data)
        tempfile.close()

        if (finding_the_topics(extracted_data,items) == True):
            items['text'] = extracted_data
            items['valid'] = True
                
        else:
            pass
                    
    except (Exception,RuntimeError) as e: 
        print(e)
        raise

    return items
    

def parsing_doc_ext_as_pdf(url,response,items):
    """ Parses the text if the extension is pdf """

    try:
        response = requests.get(url)
        with fitz.open(stream=response.content, filetype="pdf") as doc:
            text = ''
            for page in doc:
                text += page.getText()

        if (finding_the_topics(text,items) == True):

            items['text'] = text
            items['valid'] = True

        else:
            pass

    except (Exception,RuntimeError) as e: 
        print(e)
        raise

    return items

def parsing_doc_ext_as_html(response,items):
    """ Parses the text if the extension is html """
    try:
        html = response.body
        soup = BeautifulSoup(html,"html.parser")
        text =  soup.get_text()

        if (finding_the_topics(text,items)== True):

            items['text'] = text
            items['valid'] = True
        else:
            pass

    except Exception as e: 
        print(e)
        raise

    return items

def finding_the_topics(text,items):
    """ This method retrievs the topics for each document and searches each topic with list of key words.If the keywords match is
    greater than 1. Then that respective documents is saved to database """

    model = lda_Model(text)

    topics= model.show_topics(formatted=True, num_topics=15, num_words=20)
    topics_strng = ''.join(str(e) for e in topics)
    items['topics'] = topics_strng

    words = ["artificialintelligence","intelligence","smart","autonoums","ai","informationtechnology",
    "smartcities","intelligent","sensors","smartest","gps","smartparking","businessintelligence","dataanalytics","machinelearning",
    "deeplearning","computervision","nlp","analytics","rover","technology","autonomous","drones","smarthome"]

    score =0
    macthed_words = []

    # https://www.machinelearningplus.com/nlp/topic-modeling-gensim-python/#10removestopwordsmakebigramsandlemmatize
    for i in range(num_topics):
        wp = model.show_topic(i, topn=20)
        topic_keywords = ", ".join([word.replace("_",",") for word, prop in wp])
        topic_keywords = topic_keywords.split(",")
        
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
                        


def lda_Model(text):
    """ This method performs topic modelling using LDA for each documents scarped. """

    sentences = sent_tokenize(text)

    tokens_sentences =  [word_tokenize(sentence) for sentence in sentences]

    POS_tokens = [pos_tag(tokens) for tokens in tokens_sentences]

    lemmatizer = WordNetLemmatizer()

    tokens_sentences_lemmatized = [
    [
        lemmatizer.lemmatize(el[0], get_wordnet_pos(el[1])) 
        if get_wordnet_pos(el[1]) != '' else el[0] for el in tokens_POS
    ] 
    for tokens_POS in POS_tokens
    ]
    
    stopwords_verbs = ['say', 'get', 'go', 'know', 'may', 'need', 'like', 'make', 'see', 'want', 'come', 'take', 'use', 'would', 'can','and']
    stopwords_other = ['one', 'image', 'getty', 'de', 'en', 'caption', 'also', 'copyright', 'something','browser','contact','us','telephone','email','phone','home',
    'sitemap','map','like','tweet','subscribe','alerts','share','things','terms','register','apply']
    my_stopwords = stopwords.words('english') + stopwords_verbs + stopwords_other

    tokens = list(map(lambda tokens: [token.lower() for token in tokens if token.isalpha() 
                                                and token.lower() not in my_stopwords and len(token)>=1],tokens_sentences_lemmatized))

    bigram_model = Phrases(tokens)
    tokens = list(bigram_model[tokens])                                        

    dictionary_LDA = corpora.Dictionary(tokens)

    corpus = [dictionary_LDA.doc2bow(tok) for tok in tokens]

    #loading lda model
    lda_model = models.LdaModel.load("lda.model")
    vector = lda_model[corpus] 

    return vector

    

def get_wordnet_pos(treebank_tag):

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