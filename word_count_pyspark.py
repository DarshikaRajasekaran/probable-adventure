from pyspark import SparkConf,SparkContext
from collections import Counter
from collections import OrderedDict
conf=SparkConf().setAppName("Wordcnt-2")
sc=SparkContext(conf=conf)
import string
import nltk
from nltk.corpus import stopwords
nltk.download("stopwords")

def Analyze_one_line(eachline):
    rating=eachline.split("^")[-1]
    comment=eachline.split("^")[-2].encode('utf-8').lower().translate(None,string.punctuation).strip()
    wcount=Counter(comment.split(" "))
    return(rating,wcount)

reviews=sc.textFile("Amazon_Comments.csv")
ratings=reviews.map(Analyze_one_line)
print ratings.collect()
rdd=ratings.reduceByKey(lambda x,y:x+y)
print rdd.collect()
rdd1=rdd.map(lambda x:(x[0],OrderedDict(sorted(x[1].items(),key=lambda t: t[1],reverse=True)))).sortByKey()
stop=set(stopwords.words('english'))
rf_actual=rdd1.map(lambda x:(x[0],[i for i in x[1] if i not in stop][:10]))
print rf_actual.collect()

for key,value in rf_actual.collect():
    print "%s str rating: %s"%(key,value)
