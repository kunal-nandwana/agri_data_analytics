package common

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}


object CommonUtilities {

  def getSparkSession:SparkSession={
    val sparkSession=SparkSession.builder()
      .appName("Agri-data-analytics")
      .config("spark.hadoop.google.cloud.auth.service.account.enable", true)
      .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", Constants.gcpConfigKeyPath)
      .getOrCreate()
    sparkSession
  }

  def agricultureSchema: StructType = {
    val schema = List(
      StructField("State_name", StringType, true),
      StructField("District_name", StringType, true),
      StructField("Market_name", StringType, true),
      StructField("Commodity", StringType, true),
      StructField("Variety", StringType, true),
      StructField("Group", StringType, true),
      StructField("Arrivals_Tonnes", FloatType, true),
      StructField("Min_Price_Quintal", IntegerType, true),
      StructField("Max_Price_Quintal", IntegerType, true),
      StructField("Modal_Price_Quintal", IntegerType, true),
      StructField("Reported_date", StringType, true)
    )
    StructType(schema)
  }

  def QualityDataFrame(sourcePath:String):DataFrame={
    /** Validate Data for Certain Rule **/
        /** Check for Duplicates **/
        /** Check for Null values on each and every colum */
        /** Numeric Column should not zero */

    val datePattern = """(\d{2})\s(\w{3})\s(\d{4})"""
    val sourceDf=getSparkSession.read.format("csv").option("header","true").schema(agricultureSchema).option("path",sourcePath).load()

    val nonDuplicateDf=sourceDf.dropDuplicates()

    val groupedDf = sourceDf.groupBy(sourceDf.columns.map(col): _*).count()

    val duplicateCount = groupedDf.filter(col("count") > 1).count()

    println(s"Number of duplicate rows: $duplicateCount")

    nonDuplicateDf.createOrReplaceTempView("nonDuplicateView")

    val filterdDf=getSparkSession.sql("select * from nonDuplicateView where Arrivals_Tonnes!=0 and Min_Price_Quintal!=0 & Max_Price_Quintal!=0 & Modal_Price_Quintal!=0 and State_name!='-' ")

    val optimisedDf = filterdDf.withColumn("dateComponents", regexp_extract(col("Reported_date"), datePattern, 0))
      .withColumn("Reported_date", to_date(from_unixtime(unix_timestamp(col("dateComponents"), "dd MMM yyyy"))))
      .drop("dateComponents")

    optimisedDf

  }



}
