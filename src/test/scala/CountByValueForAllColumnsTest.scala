import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FunSuite, Matchers}

class CountByValueForAllColumnsTest extends FunSuite with Matchers {

  case class TestRecord(text: String, number: Long, anotherNumber: Float)

  test("testCountByValueForAllColumns") {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val dataFrame = sqlContext.createDataFrame(Seq(
      TestRecord("A", 1, 1),
      TestRecord("B", 1, 1),
      TestRecord("A", 2, 1),
      TestRecord("A", 1, 2),
      TestRecord("C", 1, 1),
      TestRecord("A", 3, 1),
      TestRecord("A", 1, 3)
    ))

    val countByValuesForColumns =
      DeepFunctions.CountByValueForAllColumnsAggregator.execute(dataFrame)

    countByValuesForColumns shouldEqual Map(
      "text" -> Map(
        "A" -> 5,
        "B" -> 1,
        "C" -> 1
      ),
      "number" -> Map(
        "1" -> 5,
        "2" -> 1,
        "3" -> 1
      ),
      "anotherNumber" -> Map(
        "1.0" -> 5,
        "2.0" -> 1,
        "3.0" -> 1
      )
    )
  }

}
