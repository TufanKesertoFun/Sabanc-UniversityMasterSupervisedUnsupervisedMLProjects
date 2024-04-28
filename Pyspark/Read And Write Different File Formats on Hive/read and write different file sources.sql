spark-shell
/*df from csv*/
val df = spark.read.csv("/user/mustafa.keser/tufan/world.txt").createOrReplaceTempView("data");
spark.sql("create table tufan.datacsv as select * from data");


/*df from json*/
val df = spark.read.csv("/user/mustafa.keser/tufan/world.txt")
df.write.mode("overwrite").json("/user/mustafa.keser/tufan/world.json")
val df = spark.read.json("/user/mustafa.keser/tufan/world.json").createOrReplaceTempView("data");
spark.sql("create table tufan.dffromjson as select * from data").show();




/*df from parquet*/
val df = spark.read.csv("/user/mustafa.keser/tufan/world.txt");
df.write.mode("overwrite").parquet("/user/mustafa.keser/tufan/world.parquet");
val df = spark.read.parquet("/user/mustafa.keser/tufan/world.parquet").createOrReplaceTempView("data");
spark.sql("create table tufan.dffromparquet as select * from data")
spark.sql("select * from data").show();



/*df from avro*/
val df = spark.read.csv("/user/mustafa.keser/tufan/world.txt");
df.write.mode("overwrite").format("avro").save("/user/mustafa.keser/tufan/world")
val data = spark.read.format("avro").load("/user/mustafa.keser/tufan/world").createOrReplaceTempView("data")
spark.sql("create table tufan.dffromavro2 as select * from data")
spark.sql("select * from tufan.dffromavro2").show();






XML PARSING
-----------------------------------------------------------------
ADD JAR /home/mustafa.keser/tufan/hivexmlserde-1.0.0.0.jar;



CREATE TABLE tufan.book_details2(TITLE STRING, AUTHOR STRING,COUNTRY STRING,COMPANY STRING,PRICE FLOAT,YEAR INT)
ROW FORMAT SERDE 'com.ibm.spss.hive.serde2.xml.XmlSerDe'
WITH SERDEPROPERTIES (
"column.xpath.TITLE"="/BOOK/TITLE/text()",
"column.xpath.AUTHOR"="/BOOK/AUTHOR/text()",
"column.xpath.COUNTRY"="/BOOK/COUNTRY/text()",
"column.xpath.COMPANY"="/BOOK/COMPANY/text()",
"column.xpath.PRICE"="BOOK/PRICE/text()",
"column.xpath.YEAR"="/BOOK/YEAR/text()")
STORED AS INPUTFORMAT 'com.ibm.spss.hive.serde2.xml.XmlInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'
TBLPROPERTIES ("xmlinput.start"="<BOOK","xmlinput.end"= "</BOOK>");

load data local inpath'/home/mustafa.keser/tufan/books.xml' into table tufan.book_details2;

select * from tufan.book_details;

'/home/mustafa.keser/tufan/books.xml'