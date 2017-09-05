from pyspark import SparkConf, SparkContext


conf1=SparkConf()
conf1.setMaster('local')
conf1.setAppName("First Program")
sc=SparkContext(conf=conf1)

Rdd=sc.textFile("regs.txt")
data=Rdd.collect()
for record in data:
    print(record)

print()

def records(x):
    info=x.split("|")
    if (info[4]=='M'):
        info[4]='Male'
    elif (info[4]=='F'):
        info[4]='Female'    

    return info

Rdd2=Rdd.map(records)
data2=Rdd2.collect()

for record in data2:
    print(record)

def checkF(x):
    info=x
    if (info[4]=='Male'):
        return False
    elif (info[4]=='Female'):
        return True

def checkM(x):
    info=x
    if (info[4]=='Male'):
        return True
    elif (info[4]=='Female'):
        return False        

Rdd3=Rdd2.filter(checkM)
data3=Rdd3.collect()

print()

for record in data3:
    print(record)



def results(x):
    records=x.split("|")
    marks=(float(records[3])/150)*100
    marks=round(marks,0)
    if(marks<60):
        result="Fail"
    else:
        result="Pass"
    return (records[1],marks,result)



Rddname=Rdd.map(results)
dataResults=Rddname.collect()

for record in dataResults:
    print(record)


print()
def multiResults(x):
    records=x.split("|")
    marks=(int(records[3])*0.6)+(int(records[4])*0.3)+(int(records[5])*0.1)
    mark=round((marks/150)*100,0)
    if(mark<60):
        result="Fail"
    else:
        result="Pass"
    return (records[1],mark,result)    

multiRes=Rdd.map(multiResults)
dat=multiRes.collect()

for record in dat:
    print(record)
