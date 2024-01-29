import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.mutable.ArrayBuffer

// please don't change the object name
object Task3 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 3")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))
    // modify this code
    val output = textFile.map(line => line.split(",", -1).drop(1))
                 .flatMap(x => {
                    var ab = new ArrayBuffer[(Int,Int)]()
                    for ( i <- 0 until x.length ) {
                        if (x(i) != "") {
                            ab += ((i+1, 1))
                        } else {
                            ab += ((i+1, 0))
                        }
                    }
                    ab
                 })
                 .reduceByKey(_ + _)
         		 .map(x => x._1 + "," + x._2)

    output.saveAsTextFile(args(1))
  }
}


