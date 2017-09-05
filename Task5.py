from pyspark import SparkConf, SparkContext


conf1=SparkConf()
conf1.setMaster('local')
sc=SparkContext(conf=conf1)

trainees=sc.textFile("trainees.txt")
trainers=sc.textFile("trainers.txt")

both=trainees.intersection(trainers)

show=both.collect()
print(show)