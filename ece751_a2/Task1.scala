import org.apache.spark.{SparkContext, SparkConf}

// please don't change the object name
object Task1 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 1")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))

    // modify this code
    val output = textFile.map(line => line.split(",", -1))
                         .map(x => {
                         
                          var result = new StringBuilder("")
                          var max = 1
                          for (i <- 1 until x.length) 
                          { 
                            if (x(i).length != 0)
                            {
                              if (x(i).toInt > max) 
                              { 
                                max = x(i).toInt
                                
                                result.clear()
                                
                                result.append(i)
                              }
                              else if (x(i).toInt == max) 
                              {
                                result.append(",").append(i)
                              }
                            }
                          }
                          new StringBuilder("").append(x(0)).append(",").append(result.toString)
                         })

    output.saveAsTextFile(args(1))
  }
}
