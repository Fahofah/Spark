from pyspark import SparkConf, SparkContext

conf1=SparkConf()
sc=SparkContext(conf=conf1)

Rdd=sc.textFile("numbers.txt")
headings=Rdd.first()


def sep(x):
    dat=x.split(",") 
    return int(dat[0]),int(dat[2])


def failsubjects(subject1,subject2):
        W=0
        if subject1<60:
            W=W+1
        if subject2<60:
            W=W+1
        return W

Rdata=Rdd.filter(lambda x: x!=headings).map(sep)
Rtest=sc.textFile("Trainees.txt")
#print( Rtest.collect())
#print (Rdata.reduceByKey(failsubjects).collect())



RfailedExams=Rdata.filter(lambda x: x[1]<65)
failedExams=RfailedExams.countByKey()
allExams=Rdata.countByKey()

def addFails(x):
     failedSum=failedExams[x[0]]
     return x[0],x[1],failedExams

RdatawdithFailed=Rdata.map(addFails).collect()

for record in RdatawdithFailed:
    print(record)


#print(allExams[])
#print(type(Rtest))
#print(type(failedExams))

#R1= Rtest.join(failedExams)



def result(x):
    if (x[1]==0):
        x[1]="Pass"
    elif (x[1]==1):
        x[1]="Repeat Exam"
    elif (x[1]==2):
        x[1]="Repeat Course"         
    elif (x[1]==3):
        x[1]="Go Home"




