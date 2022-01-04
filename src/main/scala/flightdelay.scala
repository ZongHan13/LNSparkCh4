import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{desc, when}
import org.apache.spark.sql.functions.{col, concat, udf}
import org.apache.spark.sql.functions.{to_date, to_timestamp}
import org.apache.spark.sql.types.{StringType, LongType}
import org.apache.spark.sql.functions.{
  unix_timestamp,
  from_unixtime,
  date_format,
  to_date
}
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Column
import java.text.SimpleDateFormat
import java.lang.annotation.Annotation
import java.lang.reflect.Type
import java.sql.Date

object flightdelay extends App {
  val spark = SparkSession.builder
    .master("local[*]")
    .config("spark.driver.host","127.0.0.1")
    .config("spark.driver.bindAddress","127.0.0.1")
    .appName("flightSQLExample")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._
  val schema =
    "date STRING, delay INT, distance INT, origin STRING, destination STRING"
  val csvfile = spark.read
    .format("csv")
    .schema(schema)
    .option("header", true)
    .load("/workspace/LNSparkCh4/departuredelays.csv")
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

  /*spark.sql("""
    SELECT distance, origin, destination
    FROM us_delay_flights_tbl WHERE distance > 1000
    ORDER BY distance DESC
    """).show(10) */
  // i try in another way
  println(
    "--------------------------- in spark scala way--------------------------"
  )
  /*csvfile.select("distance", "origin", "destination")
  //.where($"distance" > 1000)
  .where("distance > 1000")
  .orderBy(desc("distance"))
  .show(10) */

  //find all flights between San Framcisco(SFO) amd Chicago (ORD) with at least a two hour delay
  /*spark.sql("""
    SELECT date, delay, origin, destination
    FROM us_delay_flights_tbl
    WHERE delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD'
    ORDER by delay DESC
    """).show(10)*/

  //try in another way
  /*csvfile.select("date","delay", "origin", "destination")
    .where(col("delay") > 120 && col("origin")=== "SFO" && col("destination")=== "ORD")
    .orderBy(desc("delay"))
    .show(10)*/

  //val covertoString = csvfile.withColumn("stringDate",col("date").cast(StringType)).drop("date")
  //covertoString.select("stringDate","delay","distance","origin","destination").show(10)
  //println(covertoString.printSchema())
  //val convert = csvfile.select(col("date").cast(LongType))
  //convert.show(10)
  //println(convert.printSchema())

  val sdf = new SimpleDateFormat("MM-dd HH:mm")
  /*val parsedatefunc = udf{((date: String) => "1993-".concat(date.substring(0,2))
    .concat("-").concat(date.substring(2,4))
    .concat(" ").concat(date.substring(4,6))
    .concat(":").concat(date.substring(6,8)))}*/
  val parsedatefunc_1_5 = udf {
    (
        (date: String) =>
          date
            .substring(0, 2)
            .concat("-")
            .concat(date.substring(2, 4))
            .concat(" ")
            .concat(date.substring(4, 6))
            .concat(":")
            .concat(date.substring(6, 8))
    )
  }
  val parsedatafunc2 = udf((date: String) => sdf.parse(date).toString())

  //val data1 = csvfile.select(col("date"))
  //data1.show(10)
  //val formatDate = csvfile.select("date").map(date => date.getString(0).splitAt(2)).toDF("MM","DD")
  //formatDate.show()
  //val formatDate2 = formatDate.select("MM","DD").map(date => date.getString(0).concat("-"))
  //formatDate2.show(10)
  //
  //val finaldate2 = csvfile.select(parsedatefunc(col("date")).as("parsedDate"))
  //finaldate2.show(10)
  //val parsedate = csvfile.withColumn("parseddate2", parsedatefunc(col("date"))).drop("date")
  //parsedate.show(10)
  val parsedate2 =
    csvfile.withColumn("parsedate3", parsedatefunc_1_5(col("date")))
  parsedate2.show()
  println(parsedate2.printSchema())
  val formattered = parsedate2.select(
    col("parsedate3"),
    parsedatafunc2(col("parsedate3")).as("formatter")
  )
  formattered.show(10)
  println(formattered.printSchema())
  /*val datetype1 = parsedate.select(col("parseddate2"), date_format(col("parseddate2"),"MM-dd HH:mm a").as("formattedDate"))
  datetype1.show(10)
  println(datetype1.printSchema())
  val datetype2 = parsedate.select(col("parseddate2"), to_date(col("parseddate2"), "yyyy-MM-dd HH:mm"))
  datetype2.show(10)
  println(datetype2.printSchema())*/

  //val datetype3 = parsedate.select(col("parseddate2"), )
  //datetype2.show(10)
  //println(datetype2.printSchema())

  //println(formatDate.printSchema())

  /* Let's try a more complicated where we use the CASE clause in SQL
  we  want  to  label  all  US  flights,  regardless  of  origin  and  destination,
  with  an  indication  of  the  delays  they  experienced:  Very  Long  Delays  (>  6  hours),Long Delays (2–6 hours),
  etc. We’ll add these human-readable labels in a new columncalled Flight_Delays
   */

  spark
    .sql("""
            SELECT delay, origin, destination,
            CASE 
                WHEN delay > 360 THEN 'Very Long Delays'
                WHEN delay > 120 AND delay < 360 THEN 'Long Delays'
                WHEN delay > 60 AND delay < 120 THEN 'Tolerable Delays'
                WHEN delay = 0 THEN 'No Delays'
                ELSE 'Early'
            END AS Flingt_Delays
            FROM us_delay_flights_tbl
            ORDER BY origin, delay DESC                      
            """)
    .show(10)

  csvfile
    .select("delay", "origin", "destination")
    .withColumn(
      "Flight_Delay",
      when(
        csvfile("delay") > 360,
        "Very Long Delays"
      ) // with a new colunm named Flight Dealy and accroding to delay value to produce condition
        .when(csvfile("delay") > 120 && csvfile("delay") < 360, "Long Dealys")
        .when(
          csvfile("delay") > 60 && csvfile("delay") < 120,
          "Tolearble Delays"
        )
        .when(csvfile("delay") === 0, "No delays")
        .otherwise("Early")
    )
    .orderBy(col("origin"), col("delay").desc) //
    .show(10)
  Thread.sleep(60000)
  spark.stop()
}
