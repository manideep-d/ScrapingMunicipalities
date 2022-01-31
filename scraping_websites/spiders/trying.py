from nltk.tag import pos_tag

sentence = "smart city projects in richmondhill"
tagged_sent = pos_tag(sentence.split())
print(tagged_sent)
# [('Michael', 'NNP'), ('Jackson', 'NNP'), ('took', 'VBD'), ('Daniel', 'NNP'), ("Jackson's", 'NNP'), ('hamburger', 'NN'), ('and', 'CC'), ("Agnes'", 'NNP'), ('fries', 'NNS')]

propernouns = [word for word,pos in tagged_sent if pos == 'NN' or pos == 'NNS' ]
# ["Jackson's", "Agnes'"]
print(propernouns)