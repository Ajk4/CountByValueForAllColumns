import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FunSuite, Matchers}

class CountByValueForAllColumnsPerformanceTest extends FunSuite with Matchers {

  val testExamples = Seq(
    TestRecord("A", 1, 1),
    TestRecord("B", 1, 1),
    TestRecord("A", 2, 1),
    TestRecord("A", 1, 2),
    TestRecord("C", 1, 1),
    TestRecord("A", 3, 1),
    TestRecord("A", 1, 3)
  )

  test("testCountByValueForAllColumns") {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val data = (1 to 5000000).map(i => testExamples(i % testExamples.size))
    val dataRdd = sc.parallelize(data, 24)
    val dataFrame = sqlContext.createDataFrame(dataRdd).cache()

    // warmup
    dataFrame.foreach(_ => ())

    timed("CountByValueForAllColumns") {
      DeepFunctions.CountByValueForAllColumnsAggregator.execute(dataFrame)
    }
    timed("RDD Count") {
      // Code-generation on dataFrame.count() takes much time
      dataFrame.rdd.count()
    }

  }

  def timed[T](name: String)(code: => T): T = {
    val timeStart = System.currentTimeMillis()
    val result = code
    val timeEnd = System.currentTimeMillis()
    info(s"$name in millis: ${timeEnd - timeStart}")
    result
  }

}
