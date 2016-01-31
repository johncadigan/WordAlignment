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



object IBM1 {
  var hash = 0;
  var width = 0
  
  
  def getIndex(Is: Int, It:Int): Long = {
      It*width + Is
  }
  
  def getIndex(T: Tuple2[Int,Int]): Long = {
      T._2*width + T._1
  }
  
  def helper(pair: Tuple2[Array[Int],Array[Int]], prob: MMap[Long, Double]): Array[Tuple2[Long,Double]] = {
          
          //imperative style
          val sSize = pair._1.size
          val tSize = pair._2.size
          var res = Array.fill(sSize*tSize * 2)((0.toLong,0.0))
          var index = 0;
          var deltas = MMap[Long, Double]().withDefaultValue(0.0)
          for(j <-0 to tSize-1){
               var delta = 0.0
            for(i <-0 to sSize-1){
               delta += prob(getIndex(pair._1(i),pair._2(j)))
            }
            for(i <-0 to sSize-1){
                val ef = getIndex(pair._1(i),pair._2(j))
                val f =  getIndex(pair._1(i),hash)
                val pef = prob(ef)/delta
                res(index) = (ef, pef)
                res(index+1) = (f, pef)
                index+=2
            }
          }
          return res;
          
          /* WORKS
          val combos = for(x<-pair._1; y <- pair._2) yield(x,y)    
          /*
           *delta is the total possible probability mass for a target word 
           * 
           */
          val deltas = combos.groupBy(x=>x._2).map(x=>(x._1, x._2.map(y=>prob(getIndex(y))).reduce(_+_))) 
          val ef = combos.map(x=>(getIndex(x),prob(getIndex(x))/deltas(x._2)))
          val f = combos.map(x=>(getIndex(x._1,hash),prob(getIndex(x))/deltas(x._2)))//divide current translation by prob mass per sentence
          val rf = f.groupBy(x=>x._1).map(x=>(x._1, x._2.map(y=>y._2).reduce(_+_))).toArray
          return  ef ++ rf;
          */
  }
  
  
  def alignments(f:String, combined : Array[Tuple2[Array[Int], Array[Int]]],  prob: MMap[Long, Double]){
          val mw = new BufferedWriter(new FileWriter(new File(f)))
          
          for(i <- combined){
             val source = i._1
             val target = i._2
             var s = ""
             var pairs = Map[Tuple2[Int,Int],Int]().withDefaultValue(0)
             for(x<- 0 to source.size-1){
                val al = (0 to target.size-1).map(y=>(prob(getIndex(source(x),target(y))),x,y)).maxBy(x=>x._1)
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
    val fs = Source.fromFile(args(0))
    val ft = Source.fromFile(args(1))
    val source = fs.getLines().map(x=>x.stripLineEnd.split("\\s+")).toArray
    //val target = ft.getLines().map(x=>x.stripLineEnd.split("\\s+"):+null_value).toArray
    val target = ft.getLines().map(x=>x.stripLineEnd.split("\\s+")).toArray
    
    /*Source */
    val source_words = source.par.flatMap(x=>x).toSet.toArray.sorted
    val source_map = source_words.zipWithIndex.toMap
    width = source_map.size
    
    /*Target*/
    //val target_words = (target.flatMap(x=>x.filter(y=>y!=null_value)).toSet.toArray.sorted:+null_value)
    val target_words = (target.flatMap(x=>x.filter(y=>y!=null_value)).toSet.toArray.sorted)
    val target_map = target_words.zipWithIndex.toMap
    
    hash = target_map.size
    val source_sents = source.map(x=>x.map(y=>source_map(y)))  
    val target_sents = target.map(x=>x.map(y=>target_map(y)))
    
    val combos =  source_sents.zip(target_sents).par.flatMap(x=>x._1.flatMap(y=>(x._2).map(z=>(y,z)))).seq.toSet.toArray     

    val possible_translations = combos.groupBy(x=>x._1).map(x=>(x._1, x._2.size.toDouble)) //target
    val combined = sc.parallelize(source_sents.zip(target_sents))
    var probs = MMap(combos.map(x=>(getIndex(x._1,x._2),1.0/possible_translations(x._1))).seq.toSeq: _*)
    val cutoff = hash*width
    for(it <- 1 to args(2).toInt){    
       
        var expectation = combined.flatMap(pair=>{
            //imperative style
          val sSize = pair._1.size
          val tSize = pair._2.size
          var res = Array.fill(sSize*tSize * 2)((0.toLong,0.0))
          var index = 0;
          var deltas = MMap[Long, Double]().withDefaultValue(0.0)
          for(j <-0 to tSize-1){
               var delta = 0.0
            for(i <-0 to sSize-1){
               delta += probs(getIndex(pair._1(i),pair._2(j)))
            }
            for(i <-0 to sSize-1){
                val ef = getIndex(pair._1(i),pair._2(j))
                val f =  getIndex(pair._1(i),hash)
                val pef = probs(ef)/delta
                res(index) = (ef, pef)
                res(index+1) = (f, pef)
                index+=2
            }
          }
          res;
           
        }).reduceByKey(_+_).collect().toMap 
        var parts = expectation.par.partition(x=>x._1< cutoff)
        probs = MMap(parts._1.map(x =>(x._1,x._2/parts._2(x._1%width+cutoff ))).seq.toSeq:_*)
         
    }
    alignments(args(3), combined.toArray,probs)
        
    
    
    val end = System.currentTimeMillis()
   
    println("Total running time: %s seconds".format((end-start)/1000))
  }
}
