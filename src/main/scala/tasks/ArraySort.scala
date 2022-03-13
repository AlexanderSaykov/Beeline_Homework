package tasks

import org.apache.spark.sql.functions.{array_max, col, udf}
import org.apache.spark.sql.types.{ArrayType, IntegerType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * Этот объект содержит два метода, которые позволяют отсортировать датафрейм по массиву в колонке */

object ArraySort extends traits.SparkSession {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.master("local").getOrCreate()

    val arrayStructureData = Seq(
      Row(1, List(3, 5, 10)),
      Row(2, List(2, 20, 3)),
      Row(3, List(55, 1, 3))
    )
    val arrayStructureSchema = new StructType()
      .add("id", IntegerType)
      .add("array", ArrayType(IntegerType))

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(arrayStructureData), arrayStructureSchema)

    /**
     * Метод 1, нативный. У спарка есть встроенная функция, чтобы достать максимальное число из массива в столбце.
     * Потом по этому столбцу можно сортировать */

    def sortArrayNative(df: DataFrame): Unit = {

      df.withColumn("nums_max", array_max(col("array")))
        .orderBy(col("nums_max").desc)
        .drop("nums_max")
        .show()
    }

    /**
     * Метод 2, с UDF. Можно сначала отсортировать массив, используя UDF, а потом отсортировать весь df по нему */

    def sortArrayWithUDF(df: DataFrame): Unit = {
      val sortArray = udf { (Array: Array[Int]) => Array.sorted.reverse }

      df.withColumn("array_sorted", sortArray(col("array")))
        .orderBy(col("array_sorted").desc)
        .drop("array_sorted")
        .show()
    }

    sortArrayNative(df)

    sortArrayWithUDF(df)


  }

}
