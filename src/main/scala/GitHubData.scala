import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext

object GitHubData {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("GitHub Data").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val events = sqlContext.jsonFile("data/*.json.gz")

    events.cache()

    events.printSchema()
    events.count()

    events.registerTempTable("events")

    val forks = sqlContext.sql("""
      SELECT
        repo.name,
        count(*) AS fork_count
      FROM
        events
      WHERE
        type = 'ForkEvent'
      GROUP BY
        repo.name
      ORDER BY
        fork_count DESC
    """)

    forks.take(20).foreach(println)

    sc.stop()
  }
}
