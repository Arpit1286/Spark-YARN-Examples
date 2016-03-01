import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}


object RDDtoHiveTable {
  def main(args: Array[String]) {
    val inputFile = "/user/spark/people.txt"
    val conf = new SparkConf().setAppName("Write table to hive")

    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._

    val dateFmt = new SimpleDateFormat("yyyy-MM-dd")
    // Extract
    val rdd = sc.textFile("/user/spark/people.txt")


    // Transform
    val peopleRDD = rdd.map { line =>
      val cols = line.split(":")
      Person(firstName = cols(0),
        middleName = cols(1),
        lastName = cols(2),
        gender = cols(3),
        birthDate = new Timestamp(dateFmt.parse(cols(4)).getTime),
        salary = cols(5).toInt,
        ssn = cols(6))
    }

    val df = peopleRDD.toDF


    // load
    hiveContext.sql("CREATE TABLE IF NOT EXISTS people (firstName STRING, lastName STRING, gender STRING, birthDate Timestamp, salary INT, ssn STRING )")



    // df.write.saveAsTable("People")
  }
}

case class Person(firstName: String,
                  middleName: String,
                  lastName:   String,
                  gender:     String,
                  birthDate:  Timestamp,
                  salary:     Int,
                  ssn:        String)




