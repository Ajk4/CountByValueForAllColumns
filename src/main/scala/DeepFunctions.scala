import org.apache.spark.sql.{DataFrame, Row}

import scala.collection._

object DeepFunctions {

  object CountByValueForAllColumnsAggregator {

    type CountByValue = Map[Any, Long]
    type DistributionByColumn = Array[CountByValue]

    def execute(dataFrame: DataFrame): DistributionByColumn = {
      dataFrame.rdd.aggregate(zero(dataFrame))(seq, comb).map(_.toMap)
    }

    type Aggregator = Array[mutable.Map[Any, Long]]

    private def zero(dataFrame: DataFrame): Aggregator = {
      val distibutionsPerColumns = new Array[mutable.Map[Any, Long]](dataFrame.schema.fields.length)

      var colIndex = 0
      while(colIndex < dataFrame.schema.fields.length) {
        distibutionsPerColumns(colIndex) = mutable.Map.empty[Any, Long]
        colIndex = colIndex + 1
      }

      distibutionsPerColumns
    }

    private def seq(agg: Aggregator, row: Row): Aggregator = {
      var colIndex = 0
      while(colIndex < row.schema.fields.length) {
        val value = row(colIndex)

        val countByValueMap = agg(colIndex)
        val newCount = countByValueMap.get(value) match {
          case Some(count) => count + 1
          case None => 1
        }
        countByValueMap(value) = newCount
        colIndex = colIndex + 1
      }
      agg
    }

    private def comb(left: Aggregator, right: Aggregator): Aggregator = {
      var colIndex = 0
      while(colIndex < left.length) {
        val leftCountByValue = left(colIndex)
        val rightCountByValue = right(colIndex)
        for ((value, rightCount) <- rightCountByValue) {
          val newCount = leftCountByValue.get(value) match {
            case Some(leftCount) => leftCount + rightCount
            case None => rightCount
          }
          leftCountByValue(value) = newCount
        }
        colIndex = colIndex + 1
      }
      left
    }

  }

}
