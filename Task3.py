from pyspark import SparkConf, SparkContext


conf1=SparkConf()
sc=SparkContext(conf=conf1)

List=[1,2,3,5,10,23,5,6,12,46,7,88,123,10]

Rdd=sc.parallelize(List)
Rdd2=Rdd.filter(lambda x: x>10 )

data=Rdd2.collect()
for record in data:
    print(record)