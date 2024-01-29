import org.apache.spark.{SparkContext, SparkConf}

// please don't change the object name
object Task2 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 2")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))

    // modify this code
	val output = textFile.map(line => line.split(",", -1).drop(1))
						 .map( x => {
								var rev = 0
								for (i <- 0 until x.length)
								{
									if (x(i) != "")
									{
										rev = rev +1
									}
								}				 
								("c", rev)
						 })
						 .reduceByKey(_ + _, 1)
						 .map(x => x._2)
						 
	
    
    output.saveAsTextFile(args(1))
  }
}

