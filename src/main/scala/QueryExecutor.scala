import org.apache.log4j.LogManager
import org.apache.spark.sql.functions.{col, udf}
import session.SessionManager

object QueryExecutor {
    val log = LogManager.getLogger(this.getClass.getName)
    val sparkSession = SessionManager.createSession()

    def getFetchQuery(): String = {
        """
          | SELECT mobile
          | FROM shipping_package_address
          | WHERE uniware_sp_created >= "2024-01-01"
          |""".stripMargin
    }

    def getFilterQuery(): String = {
        """
          | SELECT count(distinct(mobile))
          | FROM raw_dataframe_view
          |""".stripMargin
    }

    def getJdbcOptions(query: String): Map[String, String] = {
        Map(
            "driver" -> "com.mysql.cj.jdbc.Driver",
            "url" -> "jdbc:mysql://db.address.unicommerce.infra:3306/turbo",
            "user" -> "developer",
            "password" -> "DevelopeR@4#",
            "query" -> query,
            "header" -> "true",
            "inferSchema" -> "true",
            "mode" -> "failfast",
            "fetchSize" -> "50000"
        )
    }

    def main(args: Array[String]): Unit = {
        log.info("=== Spark query executor ===")
        val unifillMobileNumbersFile = "/meesho/dm.csv"
        val unifillDF = sparkSession.read.options(
            Map ("header" -> "true",
                "inferSchema" -> "false",
                "mode" -> "failfast")
        ).csv(unifillMobileNumbersFile)

        // Show dataframe unique mobile
        // unifillDF.show(false)

        val addQuotesUdf = udf(StringUtils.addQuotes)
        val unifillDfSample = unifillDF.limit(100)
                .select(addQuotesUdf(col("mobile")).as("mobile"))
                .collect().mkString(",").replaceAll("[\\[\\]]","")

        print("unifillDfSample: " + unifillDfSample)


        // Execute query for small set
        // Full execution


//        val fetchQuery = getFetchQuery()
//        val jdbcOptions = getJdbcOptions(fetchQuery)
//        val filterQuery = getFilterQuery()
//
//        // Read into dataframe
//        val rawDataframe = sparkSession.read
//                .format("jdbc")
//                .options(jdbcOptions)
//                .load()
//
//        // Create temp view
//        rawDataframe.createOrReplaceTempView("raw_dataframe_view")
//        // Create dataframe from filter query
//        val outputDf = sparkSession.sql(filterQuery)
//        outputDf.show(false)
    }
}
