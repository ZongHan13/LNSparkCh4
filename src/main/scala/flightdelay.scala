import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.functions.{col}
import org.apache.spark.sql.functions.to_date
import org.apache.spark.sql.types.{StringType, LongType}
import org.apache.spark.sql.functions.{unix_timestamp, from_unixtime,date_format}
import org.apache.spark.sql.types.DateType


object flightdelay extends App{
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("flightSQLExample")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._
  val schema = "date STRING, delay INT, distance INT, origin STRING, destination STRING"
  val csvfile = spark.read.format("csv").schema(schema).option("header",true).load("/home/han/LNSparkCh4/departuredelays.csv")
    csvfile.show(10)
    println(csvfile.printSchema())
  csvfile.show(10)
  //import spark.implicits._
  
  //val csvfile2 = spark.read.schema(schema).csv("/home/han/LNSparkCh4/departuredelays.csv")
  
  //csvfile2.show(10)
  //println(csvfile2.printSchema())
  // read and create temporary view 
  csvfile.createOrReplaceTempView("us_delay_flights_tbl")
  //Let's do some quries 
  //first we’ll find all flights whose distance is greater than 1,000 miles
  spark.sql("""
    SELECT distance, origin, destination 
    FROM us_delay_flights_tbl WHERE distance > 1000 
    ORDER BY distance DESC
    """).show(10)
  // i try in another way 
  println("--------------------------- in spark scala way--------------------------")
  csvfile.select("distance", "origin", "destination")
  //.where($"distance" > 1000)
  .where("distance > 1000")
  .orderBy(desc("distance"))
  .show(10)
  
  //find all flights between San Framcisco(SFO) amd Chicago (ORD) with at least a two hour delay
  spark.sql("""
    SELECT date, delay, origin, destination
    FROM us_delay_flights_tbl
    WHERE delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD'
    ORDER by delay DESC
    """).show(10)

  //try in another way 
  csvfile.select("date","delay", "origin", "destination")
    .where(col("delay") > 120 && col("origin")=== "SFO" && col("destination")=== "ORD")
    .orderBy(desc("delay"))
    .show(10)
  
  //val covertoString = csvfile.withColumn("stringDate",col("date").cast(StringType)).drop("date")
  //covertoString.select("stringDate","delay","distance","origin","destination").show(10)
  //println(covertoString.printSchema())
  val convert = csvfile.select(col("date").cast(LongType))
  convert.show(10)
  println(convert.printSchema())

  
  val formatDate = csvfile.select(col("date"),date_format(col("date"),"MM/dd hh:mm").as("newdate"))
  formatDate.show(20)
  println(formatDate.printSchema())






  spark.stop()
}
