from pyspark import SparkConf, SparkContext

conf1=SparkConf()
sc=SparkContext(conf=conf1)

Rdd=sc.textFile("numbers.txt")
headings=Rdd.first()
Rdata=Rdd.filter(lambda x: x!=headings)

def sepMarks(x):
    dat=x.split(",") 
    return dat[0],int(dat[2])
def add(x,y):
    return x+y


Rmarks=Rdata.map(sepMarks).reduceByKey(add)
marks=Rmarks.collect()
print(headings)
for line in marks:
    print(line)

