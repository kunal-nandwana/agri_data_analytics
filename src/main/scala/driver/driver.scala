package driver
import common.CommonUtilities._
import org.apache.commons._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
object driver extends App {

  val sparkSession=getSparkSession

//  for (gsPath<-linkList){

    val gsPath="file:///home/kunal/agriculture_data/2023-09-05/Ajwan/Ajwan_01-Jan-2013_31-Aug-2023.csv"

    val sourceDf:DataFrame=QualityDataFrame(gsPath)

  val dimState: DataFrame = sourceDf
    .withColumn("State_id", abs(hash(col("State_name"))).cast(LongType)) // Change FloatType to LongType or IntegerType if needed
    .select("State_id", "State_name")
    .distinct()

     val dimDistrict: DataFrame = sourceDf
      .withColumn("district_id", abs(hash(col("District_name"))))
      .select("district_id", "District_name")
      .distinct()

    val dimMarket: DataFrame = sourceDf
      .withColumn("market_id", abs(hash(col("Market_name"))))
      .select("market_id", "Market_name")
      .distinct()

    val dimCommodity: DataFrame = sourceDf
      .withColumn("commodity_id", abs(hash(col("Commodity"))))
      .select("commodity_id", "Commodity")
      .distinct()

    val dimVariety: DataFrame = sourceDf
      .withColumn("variety_id", abs(hash(col("Variety"))))
      .select("variety_id", "Variety")
      .distinct()

    val dimGroup: DataFrame = sourceDf
      .withColumn("group_id", abs(hash(col("Group"))))
      .select("group_id", "Group")
      .distinct()

    val dimDate: DataFrame = sourceDf
      .withColumn("date_id", abs(hash(col("Reported_date"))))
      .select("date_id", "Reported_date")
      .distinct()

      val joinedDf: DataFrame = sourceDf.join(dimState, "State_name")
        .join(dimDistrict,"District_name")
        .join(dimMarket,"Market_name")
        .join(dimCommodity,"Commodity")
        .join(dimVariety,"Variety")
        .join(dimGroup,"Group")
        .join(dimDate,"Reported_date")

      val factAgriMarket=joinedDf
        .withColumn("factId",abs(hash(concat(column("State_id"),column("district_id"),column("market_id"),column("commodity_id"),column("variety_id"),column("group_id"),column("date_id"),column("Arrivals_Tonnes"),column("Min_Price_Quintal"),column("Max_Price_Quintal"),column("Modal_Price_Quintal")))))
        .select("factId","State_id","district_id","market_id","commodity_id","variety_id","group_id","date_id","Arrivals_Tonnes","Min_Price_Quintal","Max_Price_Quintal","Modal_Price_Quintal")


      val tableName:String="agriculture-data-analytics.agri_data_model.dim_state"


      dimState.printSchema()

      dimState.write.format("bigquery")
        .mode("append")
        .option("temporaryGcsBucket", "bigquerytabletemp")
        .option("table", "agriculture-data-analytics.agri_data_model.dim_state")
        .save()





//  }






}
