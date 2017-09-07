from pyspark import SparkConf, SparkContext

conf1=SparkConf()
conf1.setMaster('local')

sc=SparkContext(conf=conf1)

Rdddata=sc.textFile("regs.txt")
Rddnums=sc.textFile("numbers.txt")


def filter1(x):
    R=x.split(",")
    if(R[0]=='3'):
        return True
Rdd3=Rddnums.filter(filter1).map(lambda X: X.split(",")[1])
print (Rdd3.collect())
