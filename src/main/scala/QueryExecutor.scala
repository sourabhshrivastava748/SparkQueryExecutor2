import org.apache.log4j.LogManager
import org.apache.spark.sql.functions.{col, udf}
import session.SessionManager

object QueryExecutor {
    val log = LogManager.getLogger(this.getClass.getName)
    val sparkSession = SessionManager.createSession()

    def getTransformationQuery1(): String = {
        """
          | SELECT turbo_mobile, pincode, count(distinct(tenant_code)) as distinct_tenants
          | FROM raw_dataframe_view
          | GROUP BY turbo_mobile, pincode
          | HAVING distinct_tenants = 1
          |""".stripMargin
    }

    def getTransformationQuery2(): String = {
        """
          | SELECT turbo_mobile, pincode, count(distinct(tenant_code)) as distinct_tenants
          | FROM raw_dataframe_view
          | GROUP BY turbo_mobile, pincode
          | HAVING distinct_tenants > 1
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
            "fetchSize" -> "50000",
            "numPartitions" -> "20"
        )
    }

    def query(mobileList: String): String = {
        "select turbo_mobile, pincode, tenant_code from shipping_package_address where turbo_mobile in (" + mobileList + ")"
    }

    def queryNew(): String = {
        "select spa.turbo_mobile, spa.pincode, spa.tenant_code " +
                "from shipping_package_address spa " +
                "join temp_numbers_2023 temp on spa.turbo_mobile = temp.turbo_mobile"
    }

    def main(args: Array[String]): Unit = {
        log.info("=== Spark query executor ===")

        // Read mobile numbers
        val unifillMobileNumbersFile = "/meesho/dm.csv"
        val unifillDF = sparkSession.read.options(
            Map ("header" -> "true",
                "inferSchema" -> "false",
                "mode" -> "failfast")
        ).csv(unifillMobileNumbersFile)
        // unifillDF.show(false)

        // Create query string
        val addQuotesUdf = udf(StringUtils.addQuotes)
        val unifillDfSample = unifillDF
                .select(addQuotesUdf(col("mobile")).as("mobile"))
                .collect().mkString(",").replaceAll("[\\[\\]]","")
        // val queryString = query(unifillDfSample)
        val queryString = queryNew()
        println("queryString: " + queryString)

        // Execute query for small set
        val jdbcOptions = getJdbcOptions(queryString)
        val rawDataframe = sparkSession.read
                .format("jdbc")
                .options(jdbcOptions)
                .load()

        // Create temp view and apply transformation
        rawDataframe.createOrReplaceTempView("raw_dataframe_view")
        val output1 = sparkSession.sql(getTransformationQuery1())
        val output2 = sparkSession.sql(getTransformationQuery2())
        println("Addresses with one tenant: " + output1.count())
        println("Addresses with greater than one tenant: " + output2.count())

    }
}
