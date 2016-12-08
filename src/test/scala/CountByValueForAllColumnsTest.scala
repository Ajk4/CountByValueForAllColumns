import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FunSuite, Matchers}



class CountByValueForAllColumnsTest extends FunSuite with Matchers {

  test("testCountByValueForAllColumns") {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val data = Seq(
      TestRecord("A", 1, 1),
      TestRecord("B", 1, 1),
      TestRecord("A", 2, 1),
      TestRecord("A", 1, 2),
      TestRecord("C", 1, 1),
      TestRecord("A", 3, 1),
      TestRecord("A", 1, 3)
    )
    val dataRdd = sc.parallelize(data, 2)
    val dataFrame = sqlContext.createDataFrame(dataRdd)

    val countByValuesForColumns =
      DeepFunctions.CountByValueForAllColumnsAggregator.execute(dataFrame)

    countByValuesForColumns shouldEqual Array(
      Map(
        "A" -> 5,
        "B" -> 1,
        "C" -> 1
      ),
      Map(
        "1" -> 5,
        "2" -> 1,
        "3" -> 1
      ),
      Map(
        "1.0" -> 5,
        "2.0" -> 1,
        "3.0" -> 1
      )
    )
  }

}
