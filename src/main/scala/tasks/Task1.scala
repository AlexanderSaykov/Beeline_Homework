package tasks

import org.apache.spark.sql.functions.{avg, col, when}
import traits.SparkSession

/**
 * Задача
 * Написать spark приложение, которое в локальном режиме выполняет следующее:
 * По имеющимся данным о рейтингах книг посчитать агрегированную статистику по ним.
 *
 * 1. Прочитать csv файл: book.csv
 * 2. Вывести схему для dataframe полученного из п.1
 * 3. Вывести количество записей
 * 4. Вывести информацию по книгам у которых рейтинг выше 4.50
 * 5. Вывести средний рейтинг для всех книг.
 * 6. Вывести агрегированную инфорацию по количеству книг в диапазонах:
 * 0 - 1
 * 1 - 2
 * 2 - 3
 * 3 - 4
 * 4 - 5
 */

object Task1 extends SparkSession {

  def main(args: Array[String]): Unit = {

    val df = spark.read.option("InferSchema", true).option("header", true).csv("tables/books.csv")

    //Выводим схему
    df.printSchema()

    //Считаем количество записей
    df.count()

    //Выводим таблицу, где средний рейтинг больше 4.5
    df.filter(col("average_rating") > 4.50).show()

    //Считаем общий средний рейтинг
    df.select(avg(col("average_rating")).as("average_rating")).show()

    /*Вывести агрегированную инфорацию по количеству книг в диапазонах:
    *0 - 1
    *1 - 2
    *2 - 3
    *3 - 4
    *4 - 5 */

    df.withColumn("raiting_bucket", when(col("average_rating") >= 0 && col("average_rating") < 1, "0-1")
      when(col("average_rating") >= 1 && col("average_rating") < 2, "1-2")
      when(col("average_rating") >= 2 && col("average_rating") < 3, "2-3")
      when(col("average_rating") >= 3 && col("average_rating") < 4, "3-4")
      when(col("average_rating") >= 4 && col("average_rating") <= 5, "4-5")
      otherwise "error")
      .groupBy("raiting_bucket").count()
      .show(false)

    // Из-за неверно выбранного разделителя в csv спарк не может правильно распарсить некоторые строки. Для решения проблемы нужно выбрать другой разделитель или другой формат

  }
}
