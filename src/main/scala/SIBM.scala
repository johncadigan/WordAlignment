import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

class SIBM {
  
}


object IBM1 {
  def main(args: Array[String]) {
    val logFile = "build.sbt" // Should be some file on your system
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    val start = System.currentTimeMillis()
    
    val fs = sc.textFile(args(0), 2).cache
    val ft = sc.textFile(args(1), 2).cache()
    val text_source = fs.map(x=>x.stripLineEnd.split("\\s+"))
    val text_target = ft.map(x=>x.stripLineEnd.split("\\s+"))
    val null_value = "NULL#!@"
    val text_combos = text_source.zip(text_target).flatMap(x=>x._1.flatMap(y=>(x._2:+null_value).map(z=>(y,z)))).collect()

    //println("read files")
    //val sourceT = new IBM1(source, target)
    
    
    
    val end = System.currentTimeMillis()
    println("Total running time: %s seconds".format((end-start)/1000))
  }
}