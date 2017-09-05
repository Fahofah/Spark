from pyspark import SparkConf, SparkContext


conf1=SparkConf()
sc=SparkContext(conf=conf1)

List1=[1,2,3,4,5,6,7]
List2=[6,7,8,9,10]

Rdd1=sc.parallelize(List1)
Rdd2=sc.parallelize(List2)

Rdd3=Rdd1.union(Rdd2)
Rdd4=Rdd1.intersection(Rdd2)
Rdd5=Rdd1.substract(Rdd2)

data1=Rdd3.collect()
data2=Rdd4.collect()
data3=Rdd5.collect()

print(data1)
print(data2)
print(data3)