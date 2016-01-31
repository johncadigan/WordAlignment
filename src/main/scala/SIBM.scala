import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import scala.io.Source
import java.io.BufferedWriter
import java.io.BufferedReader
import java.io.File
import java.io.FileWriter
import java.io.FileReader

import scala.collection.mutable.{Map => MMap}
import scala.collection.mutable.MutableList
import scala.collection.mutable.HashMap


object IBM1 {
  var hash = 0;
  var width = 0
  
  
  def getIndex(Is: Int, It:Int): Long = {
      It*width + Is
  }
  
  def getIndex(T: Tuple2[Int,Int]): Long = {
      T._2*width + T._1
  }
  
  def help(pair: Tuple2[Array[Int],Array[Int]], prob: MMap[Long, Double]): Array[Tuple2[Long,Double]] = {
          
          //imperative style
          val sSize = pair._1.size
          val tSize = pair._2.size
          var res = Array.fill(sSize*tSize * 2)((0.toLong,0.0))
          var index = 0;
          var deltas = MMap[Long, Double]().withDefaultValue(0.0)
          for(j <-0 to tSize-1){
               var delta = 0.0
            for(i <-0 to sSize-1){
               delta += prob(pair._2(j)*width+pair._1(i))
            }
            for(i <-0 to sSize-1){
                val ef = pair._2(j)*width+pair._1(i)
                val f =  pair._2(j)%width+width*hash
                val pef = prob(ef)/delta
                res(index) = (ef, pef)
                res(index+1) = (f, pef)
                index+=2
            }
          }
          return res;
  }
  
  
  def alignments(f:String, combined : Array[Tuple2[Array[Int], Array[Int]]],  prob: Array[Double]){
          val mw = new BufferedWriter(new FileWriter(new File(f)))
          
          for(i <- combined){
             val source = i._1
             val target = i._2
             var s = ""
             var pairs = Map[Tuple2[Int,Int],Int]().withDefaultValue(0)
             for(x<- 0 to source.size-1){
                val al = (0 to target.size-1).map(y=>(prob(source(x)+target(y)*width),x,y)).maxBy(x=>x._1)
                if(target(al._3) < width*hash){
                  mw.write("%s-%s ".format(al._2, al._3))
                }
             }
             mw.write("\n")
          }
          mw.close()
      }
  
  def sentence_alignments(source : Array[Int], target: Array[Int], probs : MMap[Long,Double]): MMap[Tuple2[Int,Int],Int] ={
             var s = ""
             var pairs = MMap[Tuple2[Int,Int],Int]().withDefaultValue(0)
             for(x<- 0 to source.size-1){
                val al = (0 to target.size-1).map(y=>(probs(getIndex(source(x),target(y))),x,y)  ).maxBy(x=>x._1)
                if(target(al._3) < width*hash){
                  pairs((al._2, al._3)) +=1
                }
             }
             return pairs
 }
  
  def main(args: Array[String]) {
    val logFile = "sparkaligner.log" // Should be some file on your system
    val file = new File(logFile)
    val sysw = new BufferedWriter(new FileWriter(file))
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    val start = System.currentTimeMillis()
    val null_value = "NULL#!@"
    val fs = sc.textFile((args(0)))
    val ft = sc.textFile((args(1)))
    val source = fs.map(x=>x.stripLineEnd.split("\\s+")).collect().toArray
    //val target = ft.getLines().map(x=>x.stripLineEnd.split("\\s+"):+null_value).toArray
    val target = ft.map(x=>x.stripLineEnd.split("\\s+")).toArray
    
    /*Source */
    val source_words = source.par.flatMap(x=>x).toSet.toArray.sorted
    val source_map = source_words.zipWithIndex.toMap
    
    width = source_map.size
    val w = sc.broadcast(width)
    
    /*Target*/
    //val target_words = (target.flatMap(x=>x.filter(y=>y!=null_value)).toSet.toArray.sorted:+null_value)
    val target_words = (target.flatMap(x=>x.filter(y=>y!=null_value)).toSet.toArray.sorted)
    val target_map = target_words.zipWithIndex.toMap
    
    hash = target_map.size
    val h = sc.broadcast(hash)
    val source_sents = source.map(x=>x.map(y=>source_map(y)))  
    val target_sents = target.map(x=>x.map(y=>target_map(y)))
    
    val combos =  source_sents.zip(target_sents).par.flatMap(x=>x._1.flatMap(y=>(x._2).map(z=>(y,z)))).seq.toSet.toArray     

    val possible_translations = combos.groupBy(x=>x._1).map(x=>(x._1, x._2.size.toDouble)) //target
    val combined = sc.parallelize(source_sents.zip(target_sents))
    val probs = Array.ofDim[Double](source_map.size*(target_map.size+1))
    combos.foreach(x=>probs(x._1+x._2*width)=1.0/possible_translations(x._1))
    val cutoff = hash*width
    
    //function with a closure
    def helper(pair: Tuple2[Array[Int],Array[Int]], prob: Array[Double]): Array[Tuple2[Int,Double]] = {
          
          val sSize = pair._1.size
          val tSize = pair._2.size
          var res = Array.fill(sSize*tSize * 2)((0,0.0))
          var index = 0;
          var deltas = MMap[Int, Double]().withDefaultValue(0.0)
          for(j <-0 to tSize-1){
               var delta = 0.0
            for(i <-0 to sSize-1){
               delta += prob(pair._2(j)*w.value+pair._1(i))
            }
            for(i <-0 to sSize-1){
                val ef = pair._2(j)*w.value+pair._1(i)
                val f =  w.value*h.value+pair._1(i)%w.value
                val pef = prob(ef)/delta
                res(index) = (ef, pef)
                res(index+1) = (f, pef)
                index+=2
            }
          }
          return res;
    }
    
    
    
    
    for(it <- 1 to args(2).toInt){    
       
        //var expectation = combined.flatMap(i=>helper(i, probs)).reduceByKey(_+_).collect().toMap
        var expectation = combined.flatMap(pair=>helper(pair,probs)).reduceByKey(_+_).collect().toMap
        var parts = expectation.par.partition(x=>x._1< hash*width)
        parts._1.map(x =>(x._1,x._2/parts._2(x._1%width+cutoff))).foreach(i=>probs(i._1)=i._2)
    }
    alignments(args(3), combined.toArray,probs)
        
    
    
    val end = System.currentTimeMillis()
   
    println("Total running time: %s seconds".format((end-start)/1000))
  }
}
