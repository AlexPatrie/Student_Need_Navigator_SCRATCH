import tweepy as tw
import numpy as np
import pandas as pd 
import seaborn as sns 
from textblob import Word, TextBlob
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from scipy.special import softmax 
import matplotlib.pyplot as plt 

CONNECTION = 'postgresql://postgres:07141989@localhost:5432/my_coding_journey'

"""THIS IS PREPROCESSING SECTION AND CLEANING SECTION"""
def format_strings(word_strings):
  #using an empty string as a container vs a list as the model seems to favor
  #a raw string more
  word_list = " "
  for word in word_strings.split(' '):
    word_strings.replace('[', '').replace(']', '').replace('•', '').replace(' ', '')
    #replacing these characters with nothing, not even an empty space or replacing altogether
    if word.startswith('@') and len(word) > 1:
      word = '@user'
    elif word.startswith('http'):
      word = "http"
    word_list += f", {word}"
  formatted_string = str(word_list).lower()
  return formatted_string

#raw strings
new_tweet1 = "@realDonaldTrump is taking a moist shit @ home. I love my shitter!"
new_tweet2 = "motivated, Creative, REdirection"
new_tweet3 = "student is Very mad and sad About everything"
new_tweet4 = "@Alex just got a job as a Python developer and is so excited!"

#raw strings formatted
formatted_tweet1 = format_strings(new_tweet1)
formatted_tweet2 = format_strings(new_tweet2)
formatted_tweet3 = format_strings(new_tweet3)
formatted_tweet4 = format_strings(new_tweet4)


##load the model
roberta = "cardiffnlp/twitter-roberta-base-sentiment"

##new instance of pretrained sequence model
model = AutoModelForSequenceClassification.from_pretrained(roberta)

##create tokenizer for words:
tokenizer = AutoTokenizer.from_pretrained(roberta)

##labels taken from hugging face website
labels = ['Negative', 'Neutral', 'Positive']

def encode_fit_predict_prob(text):
#  #sentiment analysis
  enc_tweet = tokenizer(text, return_tensors='pt')
  output = model(enc_tweet['input_ids'], enc_tweet['attention_mask'])
  result_scores = output[0][0].detach().numpy()
  result_scores = softmax(result_scores)
  return result_scores

##prints label list and corresponding score
def view_sentiment_scores(label_list, score):
  for i in range(len(score)):
    l = label_list[i]
    s = score[i]
    print(f"{l} {s}")
    
#score1 is like history1...first pred
#score1 = encode_fit_predict_prob(text='fuck my ass')
#view_sentiment_scores(labels, score1)

#this will fetch and execute a sql query...select table to convert to df
sql_query = pd.read_sql_query('''
                              select * from august_14_22
                              '''
                              , CONNECTION) 

#create df from fetched sql
dframe = pd.DataFrame(sql_query)
#var contains just the FIRST row of the entry col
entries = dframe['entry'][:1]

#empty container
word_list = " "
#put rows of specified into container as A STRING BIOTCHHH!!
for rows in entries:
    word_list += rows
print(word_list)


'''1.create sql query with read_sql_query
   2.pass that thry pd.DataFrame
   3.raw string
   4.format string
   5.encode_fit_prob
   6.sentiment_scores
   7.push BACK into sql'''