from pyspark import SparkConf, SparkContext

conf1=SparkConf()
sc=SparkContext(conf=conf1)

Rdata=sc.textFile("u.data")

def sep(x):
    R=x.split("\t")
    return (int(R[1]),int(R[2]))

ge5star=Rdata.map(sep).filter(lambda x: x[0]==2).filter(lambda x: x[1]==5).count()
ge4star=Rdata.map(sep).filter(lambda x: x[0]==2).filter(lambda x: x[1]==4).count()
ge3star=Rdata.map(sep).filter(lambda x: x[0]==2).filter(lambda x: x[1]==3).count()
ge2star=Rdata.map(sep).filter(lambda x: x[0]==2).filter(lambda x: x[1]==2).count()
ge1star=Rdata.map(sep).filter(lambda x: x[0]==2).filter(lambda x: x[1]==1).count()

print("5 star: "+ str(ge5star))
print("4 star: "+ str(ge4star))
print("3 star: "+ str(ge3star))
print("2 star: "+ str(ge2star))
print("1 star: "+ str(ge1star))