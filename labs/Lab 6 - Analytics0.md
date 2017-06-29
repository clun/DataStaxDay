**[Back to Agenda](./../README.md)**

Hands On DSE Analytics
--------------------

Spark is general cluster compute engine. You can think of it in two pieces: **Streaming** and **Batch**.
**Streaming** is the processing of incoming data (in micro batches) before it gets written to Cassandra (or any database).
**Batch** includes both data crunching code and **SparkSQL**, a hive compliant SQL abstraction for **Batch** jobs.

It's a little tricky to have an entire class run streaming operations on a single cluster, so if you're interested in dissecting a full scale streaming app, check out [THIS git](https://github.com/retroryan/SparkAtScale).  

>Spark has a REPL we can play in. But to make things easy, first we'll use the Spark SQL REPL:

```dse spark-sql --conf spark.ui.port=<Pick a random 4 digit number> --conf spark.cores.max=1```

>Notice the spark.ui.port flag - Because we are on a shared cluster, we need to specify a radom port so we don't clash with other users. We're also setting max cores = 1 or else one job will hog all the resources.

Try some unfamiliar CQL commands on that Amazon data - like a sum on a column:

```
use <your keyspace name>.;
SELECT sum(price) FROM metadata;
```
You should see the following output:
```
140431.25000000006
```
Try a join on two tables:
```
SELECT m.title, c.city FROM metadata m JOIN clicks c ON m.asin=c.asin;
```
Output for example:
```
Major Legal Systems in the World Today: An Introduction to the Comparative Study of Law	San Francisco
Major Legal Systems in the World Today: An Introduction to the Comparative Study of Law	San Francisco
Major Legal Systems in the World Today: An Introduction to the Comparative Study of Law	San Francisco
...
Major Legal Systems in the World Today: An Introduction to the Comparative Study of Law	San Francisco
Major Legal Systems in the World Today: An Introduction to the Comparative Study of Law	South San Francisco
Major Legal Systems in the World Today: An Introduction to the Comparative Study of Law	San Francisco
```
Sums and groups:
```
SELECT asin, sum(price) AS max_price FROM metadata GROUP BY asin ORDER BY max_price DESC limit 1;
```
Output:
```
B0002GYI5A      899.0
```

Now let's try an excercise using the Spark REPL.
We will load a csv file into a Cassandra table.

In the repo directory:
Start cqlsh like this from the command prompt on one of the nodes in the cluster:

```
cqlsh <node private ip address>
```
or
```
cqlsh node0
```

Then :
```
USE <your keyspace name>;

CREATE TABLE albums (
    artist text,
    album text,
    year text,
    country text,
    quality text,
    status text,
    PRIMARY KEY ((artist,album, year,country))
);

cqlsh>  COPY albums FROM 'albums.csv' WITH HEADER=TRUE;
```

To exit the cqlsh, type ```exit```

Now we start the Spark REPL :
```
dse spark --conf spark.cores.max=1
```
Once in the REPL we can run some Scala commands:
```
println("Spark version:"+sc.version)
```
Now we can create a dataframe from our Cassandra table:
```
val df_albums = sqlContext.read.format("org.apache.spark.sql.cassandra").options(Map("keyspace" -> "<your keyspace name>", "table" -> "albums")).load().cache
```
>Dataframes were introduced in Spark 1.3 and are a more efficient means of managing and analysing data than traditional Spark RDD's - you can read more about them [here](https://databricks.com/blog/2015/02/17/introducing-dataframes-in-spark-for-large-scale-data-science.html) and a good explanation [here](http://stackoverflow.com/questions/31508083/difference-between-dataframe-and-rdd-in-spark)

We can view the schema of a DataFrame:
```
scala> df_albums.printSchema()
root
 |-- artist: string (nullable = true)
 |-- album: string (nullable = true)
 |-- year: string (nullable = true)
 |-- country: string (nullable = true)
 |-- quality: string (nullable = true)
 |-- status: string (nullable = true)
```
We can display some of the records (by default the first 20):
```
scala> df_albums.show()
+------------------+--------------------+----+------------------+-------+--------------+
|            artist|               album|year|           country|quality|        status|
+------------------+--------------------+----+------------------+-------+--------------+
|      Miss Platnum|               Chefa|2007|           Germany| normal|      Official|
|       Mount Eerie|             I Whale|2005|               USA| normal|      Official|
|        Jerry Reed|         Me and Chet|1972|               USA| normal|      Official|
|Insane Clown Posse|         Hokus Pokus|1998|               USA| normal|     Promotion|
|            Deluxe|Colillas en el suelo|2007|             Spain| normal|      Official|
|    The Low Anthem|2009-06-18: Daytr...|2009|           Unknown| normal|     Promotion|
|Hardcore Superstar|Mother's Love / S...|  -1|           Unknown| normal|      Official|
|         Sub Focus|              Splash|2010|    United Kingdom| normal|      Official|
|  Benjamin Diamond|      Fit your heart|  -1|           Unknown| normal|      Official|
|              Kane|       Shot of a Gun|2008|       Netherlands| normal|      Official|
|            Tiësto|Magik One: First ...|2000|       Netherlands| normal|      Official|
|       Regenerator|     Everyone Follow|1994|               USA| normal|      Official|
|    Duke Ellington|Music by Ellingto...|1986|           Unknown| normal|      Official|
|   Mott the Hoople|      The Collection|1987|            France| normal|      Official|
|          R. Kelly|       Bump N' Grind|1994|               USA| normal|      Official|
|        Duvelduvel|        Puur Kultuur|2007|       Netherlands| normal|      Official|
|       StoneBridge|        Take Me Away|2005|    United Kingdom| normal|      Official|
|     Pig Destroyer|                Demo|1997|               USA| normal|Pseudo-Release|
|         Ленинград|Мат без электриче...|1999|Russian Federation| normal|      Official|
| Jacques Offenbach|         Pomme d'api|1983|            France| normal|      Official|
+------------------+--------------------+----+------------------+-------+--------------+
```
We can FILTER the results:
```
scala> df_albums.filter("year > 2000").show()
+--------------------+--------------------+----+--------------+-------+--------------+
|              artist|               album|year|       country|quality|        status|
+--------------------+--------------------+----+--------------+-------+--------------+
|        Miss Platnum|               Chefa|2007|       Germany| normal|      Official|
|         Mount Eerie|             I Whale|2005|           USA| normal|      Official|
|              Deluxe|Colillas en el suelo|2007|         Spain| normal|      Official|
|      The Low Anthem|2009-06-18: Daytr...|2009|       Unknown| normal|     Promotion|
|           Sub Focus|              Splash|2010|United Kingdom| normal|      Official|
|                Kane|       Shot of a Gun|2008|   Netherlands| normal|      Official|
|          Duvelduvel|        Puur Kultuur|2007|   Netherlands| normal|      Official|
|         StoneBridge|        Take Me Away|2005|United Kingdom| normal|      Official|
|    Jake Shimabukuro|   Play Loud Ukulele|2005|       Unknown| normal|      Official|
|               Foals|             Cassius|2008|United Kingdom| normal|      Official|
|               Clark|       Growls Garden|2009|United Kingdom| normal|      Official|
|        Constantines|   Too Slow for Love|2009|        Canada| normal|      Official|
|The Electric Soft...|   Holes in the Wall|2002|United Kingdom| normal|      Official|
|        Farben Lehre|           Pozytywka|2003|        Poland| normal|      Official|
|        Icon of Coil|III: The Soul Is ...|2006|       Unknown| normal|      Official|
|            DJ Marky|FabricLive 55: DJ...|2011|United Kingdom| normal|      Official
|         Jay Reatard|         In the Dark|2007|       Austria| normal|      Official|
|               Tryad|            The Tree|2011|       Unknown| normal|     Promotion|
|                Kiki|         Love Kills!|2012|       Germany| normal|      Official|
+--------------------+--------------------+----+--------------+-------+--------------+
```
We can GROUP BY and COUNT:
```
scala> df_albums.groupBy("year").count().show()
+----+-----+                                                                    
|year|count|
+----+-----+
|1917|   10|
|1918|    6|
|1919|    4|
|1980|  807|
|1981|  853|
|1982| 1047|
|1983|  975|
|1984| 1007|
|1985|  998|
|1986| 1081|
|1987| 1288|
|1988| 1323|
|1989| 1505|
|1920|    4|
|1921|    1|
|1922|    5|
|1923|    4|
|1924|   18|
|1925|    6|
|1926|    9|
+----+-----+
```
We can use the DataFrame to create an in-memory Spark SQL table:
```
df_albums.registerTempTable("spark_albums_table")

sqlContext.sql("SELECT * FROM spark_albums_table").show
+------------------+--------------------+----+------------------+-------+--------------+
|            artist|               album|year|           country|quality|        status|
+------------------+--------------------+----+------------------+-------+--------------+
|      Miss Platnum|               Chefa|2007|           Germany| normal|      Official|
|       Mount Eerie|             I Whale|2005|               USA| normal|      Official|
|        Jerry Reed|         Me and Chet|1972|               USA| normal|      Official|
|Insane Clown Posse|         Hokus Pokus|1998|               USA| normal|     Promotion|
|            Deluxe|Colillas en el suelo|2007|             Spain| normal|      Official|
|    The Low Anthem|2009-06-18: Daytr...|2009|           Unknown| normal|     Promotion|
|Hardcore Superstar|Mother's Love / S...|  -1|           Unknown| normal|      Official|
|         Sub Focus|              Splash|2010|    United Kingdom| normal|      Official|
|  Benjamin Diamond|      Fit your heart|  -1|           Unknown| normal|      Official|
|              Kane|       Shot of a Gun|2008|       Netherlands| normal|      Official|
|            Tiësto|Magik One: First ...|2000|       Netherlands| normal|      Official|
|       Regenerator|     Everyone Follow|1994|               USA| normal|      Official|
|    Duke Ellington|Music by Ellingto...|1986|           Unknown| normal|      Official|
|   Mott the Hoople|      The Collection|1987|            France| normal|      Official|
|          R. Kelly|       Bump N' Grind|1994|               USA| normal|      Official|
|        Duvelduvel|        Puur Kultuur|2007|       Netherlands| normal|      Official|
|       StoneBridge|        Take Me Away|2005|    United Kingdom| normal|      Official|
|     Pig Destroyer|                Demo|1997|               USA| normal|Pseudo-Release|
|         Ленинград|Мат без электриче...|1999|Russian Federation| normal|      Official|
| Jacques Offenbach|         Pomme d'api|1983|            France| normal|      Official|
+------------------+--------------------+----+------------------+-------+--------------+
```

And now we can perform complex SQL operations on the data in Spark memory:
```
scala> sqlContext.sql("SELECT country,count(*) as nb FROM spark_albums_table group by country having count(*)>=200 order by nb").show
+------------------+----+                                                       
|           country|  nb|
+------------------+----+
|       Switzerland| 204|
|           Estonia| 207|
|          Portugal| 217|
|           Jamaica| 234|
|            Mexico| 262|
|           Austria| 318|
|           Denmark| 345|
|         Argentina| 523|
|            Turkey| 617|
|Russian Federation| 743|
|           Belgium| 757|
|            Brazil| 865|
|            Norway| 887|
|            Poland|1108|
|             Spain|1344|
|       Netherlands|1395|
|         Australia|1541|
|             Italy|1633|
|            Canada|1809|
|           Finland|1966|
+------------------+----+
```
To exit the REPL type ```exit```

You can see a great demo of running thse steps inside the Zeppelin Notebook [here](https://github.com/victorcouste/zeppelin-spark-cassandra-demo/)

**[Back to Agenda](./../README.md)**
