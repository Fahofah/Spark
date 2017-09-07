from pyspark import SparkConf, SparkContext

conf1=SparkConf()
sc=SparkContext(conf=conf1)

List1=[1,2,3,3,3,3,4,4,5,5,5,12]
Rdd=sc.parallelize(List1)
Data1=Rdd.countByValue()

for k in Data1:
    print(k,"...",Data1[k])
def add(x,y):
    return x+y

ave=float(Rdd.reduce(add))/float(Rdd.count())
print("Average: "+ str(ave))