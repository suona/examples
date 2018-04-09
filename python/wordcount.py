# Wordcount the complete works of William Shakespeare
#
# This is a common example application that is often done using Spark.  It's converted
# here to illustrate how to do things with Python and Pandas.
#
# The text file used by this example comes from Project Gutenberg
# (https://www.gutenberg.org/files/100/100-0.txt).  This file is not included with
# this code, but it can be downloaded and prepared as follows:
# - Remove all leading text before the _The Sonnets_, including the table of contents.
# - Insert a line at the beginning with the contents _text_ (no quotes).  This is what
#   the Pandas dataframe will use as the column label.
# - Remove all trailing text after _FINIS_.
#
# I re-named the resulting file _shakespeare_complete.txt_, and put it in a sibling
# directory called data.  If you alter the input file name or location, be sure to
# update the path passed to csv_read.
#
# Note that since this is a simple demo application, I haven't taken the time to fully
# prep the data.  There is a lot of additional cleaning required if you want to get a
# truly accurate count of all the words in Shakespeare's works.  This mostly consists
# of removing annotations and character names that are part of the format of the works.
import pandas as pd
from pandas import read_csv
from collections import Counter
import pprint as pp

df = pd.read_csv('../data/shakespeare_complete.txt', header=None, engine='c', names=['text'], sep='\n')
print('There are approximately', df.shape[0], 'lines in the complete works of Shakespeare.')

word_c = Counter(' '.join(df['text']).split()).most_common()

print('There are', len(word_c), 'unique words in the text.')
print('The top 10 most common words:')
pp.pprint(word_c[0:10])

word_d = dict(word_c)
print('\nThou appears', word_d['thou'], 'times in the text, and thee', word_d['thee'], 'times.')
print('There are', word_d['Hamlet'], 'occurances of the name Hamlet.')
