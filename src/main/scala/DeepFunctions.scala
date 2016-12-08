import org.apache.spark.rdd.RDD

import scala.collection._
import java.util.TreeSet

import org.apache.spark.sql.{DataFrame, Row}

object DeepFunctions {

  object CountByValueForAllColumnsAggregator {

    type CountByValue = Map[String, Long]
    type DistributionByColumn = Map[String, CountByValue]

    def execute(dataFrame: DataFrame): DistributionByColumn = {
      dataFrame.rdd.aggregate(zero(dataFrame))(seq, comb)
    }

    type Aggregator = mutable.Map[String, mutable.Map[String, Long]]

    private def zero(dataFrame: DataFrame): Aggregator = {
      val map = mutable.Map.empty[String, mutable.Map[String, Long]]

      for(field <- dataFrame.schema.fields) {
        map.put(field.name, mutable.Map.empty[String, Long])
      }

      map
    }

    private def seq(agg: Aggregator, row: Row): Aggregator = {
      for(field <- row.schema.fields) {
        val index = row.fieldIndex(field.name)
        val value = row(index).toString

        val countByValueMap = agg(field.name)
        val newCount = countByValueMap.get(value) match {
          case Some(count) => count + 1
          case None => 1
        }
        countByValueMap(value) = newCount
      }
      agg
    }

    private def comb(left: Aggregator, right: Aggregator): Aggregator = {
      for((columnName, leftCountByValue) <- left) {
        val rightCountByValue = right(columnName)
        for ((value, rightCount) <- rightCountByValue) {
          val newCount = leftCountByValue.get(value) match {
            case Some(leftCount) => leftCount + rightCount
            case None => rightCount
          }
          leftCountByValue(value) = newCount
        }
      }
      left
    }

  }

}
