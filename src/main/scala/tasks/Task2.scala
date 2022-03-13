package tasks

import org.apache.spark.sql.functions.{col, lit, substring}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import traits.SparkSession

/**
 * ## Задача
 * Написать spark приложение, которое в локальном режиме выполняет следующее:
 * По имеющимся данным о рейтингах фильмов (MovieLens: 100 000 рейтингов) посчитать агрегированную статистику по ним.
 *
 * ## Описание данных
 * Имеются следующие входные данные:
 * Архив с рейтингами фильмов.
 * Файл REDAME содержит описания файлов.
 * Файл u.data содержит все оценки, а файл u.item — список всех фильмов. (используются только эти два файла)
 * id_film=32
 *
 * 1. Прочитать данные файлы.
 * 2. создать выходной файл в формате json, где
 * Поле `“Toy Story ”` нужно заменить на название фильма, соответствующего id_film и указать для заданного фильма количество поставленных оценок в следующем порядке:
 * `"1", "2", "3", "4", "5"`. То есть сколько было единичек, двоек, троек и т.д.
 * В поле `“hist_all”` нужно указать то же самое только для всех фильмов общее количество поставленных оценок в том же порядке: `"1", "2", "3", "4", "5"`.
 */

object Task2 extends SparkSession {

  def main(args: Array[String]): Unit = {

    val uDataSchema = new StructType()
      .add("user_id", IntegerType)
      .add("item_id", IntegerType)
      .add("raiting", IntegerType)
      .add("timestamp", StringType)

    val uItemSchema = new StructType()
      .add("movie_id", IntegerType)
      .add("movie_title", StringType)
      .add("release_date", StringType)
      .add("video_release_date", StringType)
      .add("imdb_url", StringType)
      .add("unknown", StringType)
      .add("action", IntegerType)
      .add("adventure", IntegerType)
      .add("animation", IntegerType)
      .add("children's", IntegerType)
      .add("comedy", IntegerType)
      .add("crime", IntegerType)
      .add("documentary", IntegerType)
      .add("drama", IntegerType)
      .add("fantasy", IntegerType)
      .add("film_moir", IntegerType)
      .add("horror", IntegerType)
      .add("musical", IntegerType)
      .add("mystery", IntegerType)
      .add("romance", IntegerType)
      .add("sci_fi", IntegerType)
      .add("thriller", IntegerType)
      .add("war", IntegerType)
      .add("western", IntegerType)

    val uData = spark.read.option("sep", "\t").schema(uDataSchema).csv("tables/Task2/ml-100k/u.data")

    val uItem = spark.read.option("sep", "|")
      .schema(uItemSchema)
      .csv("tables/Task2/ml-100k/u.item")

    // Сначала посчитаем рейтинг для Toy Story
    var toyStory = uData.drop("timestamp").filter(col("item_id") === 1)
      .groupBy("raiting", "item_id").count()
      .join(uItem.select("movie_title", "movie_id"), uItem("movie_id") === uData("item_id"), "inner")
      .withColumnRenamed("count", "raiting_count")
      .withColumn("movie_title", substring(col("movie_title"), 0, 9))
      .orderBy(col("raiting").asc)
      .drop("movie_id", "item_id", "raiting")

    // Посчитаем рейтинг для всех фильмов
    var allFilms = uData.drop("timestamp")
      .groupBy("raiting").count()
      .withColumnRenamed("count", "raiting_count")
      .orderBy(col("raiting").asc)

    // Переведем столбец с  посчетом рейтингов в массив и положим его в первую строчку столбца для Toy Story
    toyStory = toyStory.withColumn("raiting", lit(toyStory.select("raiting_count").collect.map(_.getLong(0)))).drop("raiting_count").dropDuplicates("raiting")

    // Переведем столбец с  посчетом рейтингов в массив и положим его в первую строчку столбца для Toy Story для всех фильмов
    allFilms = allFilms.withColumn("raiting_count", lit(allFilms.select("raiting_count").collect.map(_.getLong(0))))
      .limit(1)
      .withColumn("raiting", lit("hist_all"))
      .dropDuplicates("raiting_count")
      .select("raiting", "raiting_count")

    // Объедими таблицы и сохраним их в json
    toyStory.union(allFilms).repartition(1).write.mode("overwrite").json("tables/out")

  }

}
