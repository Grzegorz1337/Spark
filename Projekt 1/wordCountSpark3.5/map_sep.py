import re

from nltk import WordNetLemmatizer, word_tokenize


def Map(r):
    lem = WordNetLemmatizer()
    sw = open("stopwords.txt", "r").readlines()
    p = re.sub(r'[^\w\s]', '', r[0]).lower()
    tokens = word_tokenize(p)
    no_sw_text = [lem.lemmatize(word) for word in tokens if word not in sw]
    return no_sw_text


print(Map(["the applicable state law.  The invalidity or unenforceability of any,1"]))