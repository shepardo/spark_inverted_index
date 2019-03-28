
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark._


object SparkWordCount {
   def main(args: Array[String]) {

      // adapted from https://stdatalabs.com/2017/03/mapreduce-vs-spark-inverted-index/
      val sc = new SparkContext( "local", "Word Count",
        "/Users/fernando.gonzalez/Downloads/java/spark-2.4.0-bin-hadoop2.7", Nil, Map())
      val inputPath = "/Users/fernando.gonzalez/Documents/src/spark/shakespeare/dataset2"

      val files = sc.wholeTextFiles(inputPath)
      val mapFiles = files.keys.zipWithUniqueId

      // As cross reference, dump the map of files
      mapFiles.saveAsTextFile("outfile_mapFiles")

      val globalMapFiles = sc.broadcast(mapFiles.collectAsMap)

      val words = files.
        flatMap {
          case (path, text) =>
            text
            .split("""\W+""")
            .map {
             // Create a tuple of (word, filePath)
             word => (word, path)
            }
          }.map {
            // Create a tuple with count 1 ((word, fileName), 1)
            case (w, p) => ((w, p), 1)
          }.reduceByKey {
            // Group all (word, fileName) pairs and sum the counts
            case (n1, n2) => n1 + n2
          }.map {
            // Transform tuple into (word, (fileName, count))
            case ((w, p), n) => (w, (p, n))
          }.groupBy {
            // Group by words
            case (w, (p, n)) => w
          }.map {
            // Output sequence of (fileName, count) into a comma separated string
            case (w, seq) =>
              val seq2 = seq map {
                case (_, (p, n)) => (globalMapFiles.value(p))
              }
              (w, "[" + seq2.toSeq.sorted.mkString(", ") + "]")
          }.sortByKey(true).zipWithUniqueId

      // As cross reference, dump the map of words
      words.map {
        case ((w, l), idx) =>
          (w, idx)
      }.saveAsTextFile("outfile_words")

      // The main dump
      words.map {
        case ((w, l), idx) =>
          (idx, l)
      }.saveAsTextFile("outfile")

      System.out.println("*** OK ***");
   }
}
