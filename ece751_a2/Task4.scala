import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.mutable.ArrayBuffer

// please don't change the object name
object Task4 {


  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 4")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))

    // modify this code
    val inputs = textFile.map(line => {
                        var movie = line.split(",", -1)
                        var ab = new Array[Int](movie.length-1)
                        for (i <- 1 until movie.length) {
                          if (movie(i) == "") {
                            ab(i-1) = 0
                          } else {
                            ab(i-1) = movie(i).toInt
                          }

                        }
                        (movie(0), ab)
                        }).collectAsMap()
    

    val broadcast = sc.broadcast(inputs)

     val inter = textFile.flatMap(x => {
                         var movie1_name = x.split(",", 2)(0)
                         //var res = new StringBuilder("");
                         var ratings1 = broadcast.value(movie1_name)

                         broadcast.value.filter(m2 => movie1_name > m2._1).map(movie2 => {
                            var count = 0
                            var ratings2 = movie2._2
                            for (i <- 0 until ratings2.size) {
                                    if (ratings2(i) == ratings1(i) && ratings2(i) != 0) {
                                        count = count + 1
                                    }
							}
                           // res.clear()
                           // res.append(movie1_name).append(",").append(movie2._1).append(",").append(count)
                            movie2._1 + "," + movie1_name + "," + count
                         })


                     })
    inter.saveAsTextFile(args(1))
  }
}
                                    
