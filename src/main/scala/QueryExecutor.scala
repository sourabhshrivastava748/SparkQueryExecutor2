import org.apache.log4j.LogManager
import org.apache.spark.sql.functions.{col, udf}
import session.SessionManager

object QueryExecutor {
    val log = LogManager.getLogger(this.getClass.getName)
    val sparkSession = SessionManager.createSession()

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

    def query(mobileList: String): String = {
        """
          | select turbo_mobile, pincode, tenant_code
          | from shipping_package_address where
          |	turbo_mobile in ( """.stripMargin + mobileList +
        """
          |	);
          |""".stripMargin
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

        println()
        println("unifillDfSample: " + unifillDfSample)
        val queryString = query(unifillDfSample)
        println()
        println("queryString: " + queryString)
        // Execute query for small set

        // val outputDf = sparkSession.sql(queryString)
        val outputDf = sparkSession.sql("show tables")
        outputDf.show(false)

    }
}
