#!/bin/python3ls
import re

from sys import stdin
from nltk.stem import WordNetLemmatizer
from nltk.tokenize import word_tokenize

#3.2.2
lemmatizer = WordNetLemmatizer()
sw = set(open("stopwords.txt", "r").readlines())

for i in stdin:
        #3.2.1
        i = re.sub(r'[^\w\s]', '', i).lower()
        tokens = word_tokenize(i)
        no_sw_text = [word for word in tokens if word not in sw]
        #3.2.2
        #3.1
        for j in no_sw_text:
                print(lemmatizer.lemmatize(j), "\t1")  #3.2.2

