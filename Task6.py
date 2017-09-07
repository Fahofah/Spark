from pyspark import SparkConf, SparkContext


conf1=SparkConf()
conf1.setMaster('local')

sc=SparkContext(conf=conf1)

Rdddata=sc.textFile("regs.txt")


numDic={1:'one',2:'two',3:'three',4:'four',5:'five',6:'six',7:'seven',8:'eight','9':'nine',10:'ten',11:'eleven',12:'twelve',13:'thirteen',14:'fourteen',15:'fifteen',16:'sixteen',17:'seventeen',18:'eighteen',19:'nineteen'}
numDic2={20:'twenty ',30:'thirty ',40:'forty ',50:'fifty ',60:'sixty ',70:'seventy ',80:'eighty ',90:'ninety '}

def towords(num):
    w=""
    if num>=100:
        w=numDic[num/100]+" hundred and "
        num=num-(num/100*100)
    if num>=20:
        w= w + numDic2[num/10*10]
        num=num-(num/10*10)
    if num<20:
        w= w + numDic[num]
    return w

def result(mark):
    res=(float(mark)/150)*100
    res=round(res,0)
    if(res<60):
        rslt="Fail"
    else:
        rslt="Pass"
    return rslt

def wordIt(x):
    line=x.split("|")
    num=line[3]
    ww=towords(int(num))
    res=result(num)
    return ( line[0],line[1],line[2],line[3],ww,res)
    

RddnumWords=Rdddata.map(wordIt)
toPrint=RddnumWords.collect()

for records in toPrint:
    print(records)


