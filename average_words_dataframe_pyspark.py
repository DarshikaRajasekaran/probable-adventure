import nltk
import numpy
import sys
import re

from pyspark.sql import Row,SQLContext
from pyspark import SparkConf,SparkContext
conf=SparkConf().setAppName("HW2Q1")
sc=SparkContext(conf=conf)
sqlContext=SQLContext(sc)



# removing punctuations
def removePunc(text):
    text=text.lower().strip()
    text=re.sub("\W+"," ",text)
    return text
        

inputs=sc.textFile("Amazon_Comments.csv").map(lambda x:x.split('^'))

# an intermediate set to remove punctuations
rev_rat_1=inputs.map(lambda x:(x[-1],removePunc(x[-2])))


review_ratings=rev_rat_1.map(lambda x:(x[0],len(x[1])))

group_com=review_ratings.groupByKey()

count_of_words=group_com.map(lambda x:(x[0],len(x[1])))
sum_of_words=review_ratings.reduceByKey(lambda x,y:(x+y))

total=count_of_words.join(sum_of_words)
avg_words=total.map(lambda x:(x[0],(x[1][1]/x[1][0])))
#print avg_words.collect()

rdd_2=avg_words.map(lambda rec: Row(Rating=float(rec[0]),Average_words=int(rec[1])))
#print rdd_2.collect()

df_1=sqlContext.createDataFrame(rdd_2)
df_1.show()


