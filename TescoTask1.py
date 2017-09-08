from pyspark.sql import SQLContext
from pyspark import SparkConf, SparkContext
from pyspark.sql import *
from pyspark.sql.types import *

conf1=SparkConf()
sc=SparkContext(conf=conf1)
sQl=SQLContext(sc)

Rcat=sc.textFile("Category.txt")
Rprod=sc.textFile("Products.txt")
Rsales=sc.textFile("Sales.txt")
RsubCat=sc.textFile("SubCategory.txt")

Hcat=Rcat.first()

Hprod=Rprod.first()

Hsales=Rsales.first()

HsubCat=RsubCat.first()


def splitSales(r):
    x=r.split(",") 
    return x[0],x[1],int(x[2]),float(x[3])

catData=Rcat.map(lambda x: x.split(",")).filter(lambda x: x!=Hcat)
prodData=Rprod.map(lambda x: x.split(",")).filter(lambda x: x!=Hprod)
salesData=Rsales.filter(lambda x: x!=Hsales).map(splitSales).collect()
subCatData=RsubCat.map(lambda x: x.split(",")).filter(lambda x: x!=HsubCat)

Hcat=Hcat.split(",")
Hprod=Hprod.split(",")
Hsales=Hsales.split(",")
HsubCat=HsubCat.split(",")

Scat= StructType(
    [
        StructField(Hcat[0],StringType()),
        StructField(Hcat[1],StringType())
        ]
)

Sprod= StructType(
    [
        StructField(Hprod[0],StringType()),
        StructField(Hprod[1],StringType()),
        StructField(Hprod[2],StringType())
        ]
)

Ssales= StructType(
    [
        StructField(Hsales[0],StringType()),
        StructField(Hsales[1],StringType()),
        StructField(Hsales[2],IntegerType()),
        StructField(Hsales[3],FloatType())
        ]
)

SsubCat= StructType(
    [
        StructField(HsubCat[0],StringType()),
        StructField(HsubCat[1],StringType()),
        StructField(HsubCat[2],StringType())
        ]
)



catDF=sQl.createDataFrame(catData,Scat)
prodDF=sQl.createDataFrame(prodData,Sprod)
salesDF=sQl.createDataFrame(salesData,Ssales)
subCatDF=sQl.createDataFrame(subCatData,SsubCat)

catDF.show()
prodDF.show()
salesDF.show()
subCatDF.show()

J= prodDF.join(salesDF,prodDF["PID"]==salesDF["PID"]).select(salesDF.PID,prodDF.SID,salesDF.QTY,salesDF.Price)
J.show()


salesDF.registerTempTable("Sales")
prodDF.registerTempTable("Products")
J.registerTempTable("Prod_Sales")
subCatDF.registerTempTable("SubCata")
catDF.registerTempTable("Cata")

query1="select max(totalSales) as M from (select count(*) as totalSales from Sales group by PID) as table1"
p=sQl.sql(query1)
t=p.collect()

query2="select PID from Sales group by PID having count(*)="+str(t[0]['M'])
t=sQl.sql(query2).collect()

query3="select Name from Products where PID="+str(t[0]['PID'])
p=sQl.sql(query3)
print ("Most Popular Item")
p.show()

q4="select max(total) as T from (select count(*) as total from Prod_Sales group by SID) as table2"
p=sQl.sql(q4)
t=p.collect()


q5="select SID from Prod_Sales group by SID having count(*)="+str(t[0]["T"])
p=sQl.sql(q5)
t=p.collect()

q6="select Title from SubCata where SID="+str(t[0]["SID"])
p=sQl.sql(q6)
print("Most Popular Sub Category")
p.show()

J2=subCatDF.join(J, subCatDF["SID"]==J["SID"]).select(J.SID,subCatDF.CID)
J2.registerTempTable("SubCata_J")

q7="select max(T) as C from (select count(*) as T from SubCata_J group by CID) as table3"
p=sQl.sql(q7)
t=p.collect()

q8="select CID from SubCata_J group by CID having count(*)="+str(t[0]["C"])
p=sQl.sql(q8)
t=p.collect()

q9="select Title from Cata where CID="+str(t[0]["CID"])
p=sQl.sql(q9)
print("Most Popular Category")
p.show()

