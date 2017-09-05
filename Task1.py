from pyspark import SparkConf, SparkContext

conf1=SparkConf()

sc=SparkContext(conf=conf1)

def DD(x):
    return 2*x

List1=[1,2,3,4,5]
Rdd=sc.parallelize(List1)
Data1=Rdd.collect()
print("List1: ",Data1)

Rdd2=Rdd.map(DD)
Data2=Rdd2.collect()

print("List2: ",Data2)
