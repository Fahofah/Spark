from pyspark import SparkConf, SparkContext

conf1=SparkConf()
sc=SparkContext(conf=conf1)

Rdata=sc.textFile("u.data")

def sep(x):
    R=x.split("\t")
    return (int(R[1]),int(R[2]))

movie_id=2
gestar=[0]*5
for i in range(0,5):
    gestar[i]=Rdata.map(sep).filter(lambda x: x[0]==movie_id).filter(lambda x: x[1]==i+1).count()
print("Movie ID: "+str(movie_id))
print("---------------------")
for i in range(4,-1,-1):
    print(str(i+1)+" star: "+ str(gestar[i]))
