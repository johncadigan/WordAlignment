package main.scala
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection.mutable.{Map => Map}
import scala.collection.immutable.{Map => IMap}
import scala.collection.mutable.Stack
import scala.collection.mutable.{Set => Set}
import scala.collection.mutable.MutableList
import scala.collection.parallel.mutable.ParMap
import scala.collection.parallel.mutable.ParSeq
import scala.collection.parallel
import scala.io.Source
import java.io.BufferedWriter
import java.io.BufferedReader
import java.io.File
import java.io.FileWriter
import java.io.FileReader

class IBM1 (sour : Array[Array[String]], tar : Array[Array[String]]){
      val text_source = sour;
      val text_target = tar;
      val hash = Int.MaxValue
      var processed = 0
      val null_value = "NULL#@!"
      var trans = Map[Tuple2[Int,Int], Double]().withDefaultValue(0.0)
      var counts = Map[Tuple2[Int,Int], Double]().withDefaultValue(0.0)
      //var combos = Set[Tuple2[String,String]]()
      //var source_vocab = Set[String]()
      //var target_vocab = Set[String]()
      val r = (0 to text_source.size-1)
      val text_combos = text_source.zip(text_target).par.flatMap(x=>x._1.flatMap(y=>(x._2:+null_value).map(z=>(y,z)))).seq.toSet
      val source_array = text_combos.map(x=>x._1).toSet.toArray
      val source_map = source_array.zipWithIndex.toMap
      
      val target_array = text_combos.map(x=>x._2).toSet.toArray
      
      val target_map = target_array.zipWithIndex.toMap
      val int_combos = text_combos.map(x=>(source_map(x._1),target_map(x._2)))
      
      val source = text_source.map(x=>x.map(y=>source_map(y)))
      val target = text_target.map(x=>x.map(y=>target_map(y)))
      println("text mapped to numbers")
      
      val possible_translations = int_combos.par.groupBy(x=>x._1).map(x=>(x._1, x._2.size.toDouble))
      println("all combinations explored")
      trans = Map(int_combos.par.map(x=>((x._1,x._2),1.0/possible_translations(x._1))).seq.toSeq: _*)
      println("initialized")
     
      
      /*def map_helper(i:Int): Array[ Tuple2[Tuple2[Int,Int], Double]] = {
          val source_sentence = source(i)
          val target_sentence = target(i)
                    
          val combos = for(x<-source_sentence; y <- target_sentence) yield(x,y)
          
          val delta = combos.groupBy(x=>x._1).map(x=>(x._1, x._2.map(y=>trans(y)).reduce(_+_))) //delta is the total possible probability mass for a target word
          return combos.map(x=>(x,trans(x)/delta(x._1))) ++ combos.map(x=>((x._2,hash),trans(x)/delta(x._1))) //divide current translation by prob mass per sentence
      }
      
      def map_train(i: Int){
        
          var it = i;
          val par_range = (0 to source.size-1).par
          while(it > 0){
                println(it)
                var expectation = par_range.flatMap(i=>map_helper(i))
                var counts = expectation.par.groupBy(x=>x._1).map(y=>(y._1, y._2.map(z=>z._2).reduce(_+_))).partition(x=>x._1._2!=hash)
                trans = Map(counts._1.map(k=>(k._1, k._2/counts._2((k._1._2,hash)))).seq.toSeq:_*)
                it-=1;
          }
        
      }*/
      
      def helper(Xs: Array[Int], Ys: Array[Int]){
          val combos = for(x<-Xs; y <- Ys) yield(x,y)
          val deltas = combos.groupBy(x=>x._1).map(x=>(x._1, x._2.map(y=>trans(y)).reduce(_+_))) //delta is the total possible probability mass for a target word
          for(x<-combos){
            val delta = trans(x)/deltas(x._1);
            counts(x)+= delta;
            counts((x._2,hash))+= delta;
          }
          
          
      }
      
      def train(i: Int){
        
          var it = i;
          val par_range = source.zip(target)
          while(it > 0){
                println(it)
                counts = Map[Tuple2[Int,Int], Double]().withDefaultValue(0.0)
                par_range.foreach(x=>helper(x._1,x._2))
               
                trans = Map(counts.filter(x=>x._1._2!=hash).seq.transform((k,v)=>v/counts((k._2,hash))).toSeq:_*)
                it-=1;
          }
        
      }

            
      def sentence_alignments(i:Int): Map[Tuple2[Int,Int],Int] ={
             val source_sentence = source(i)
             val target_sentence = target(i)
             var s = ""
             var pairs = Map[Tuple2[Int,Int],Int]().withDefaultValue(0)
             for(x<- 0 to source_sentence.size-1){
                val al = (0 to target_sentence.size-1).map(y=>(trans(source_sentence(x),target_sentence(y)),x,y)  ).maxBy(x=>x._1)
                if(target_array(target_sentence(al._3))!=null_value){
                  pairs((al._2, al._3))+=1
                }
             }
             return pairs
       }
      
       def dual_alignments(f:String, other:IBM1){
          val mw = new BufferedWriter(new FileWriter(new File(f)))
          val r = (0 to source.size-1)
          for(i <- r){
             val other_pairs = other.sentence_alignments(i)
             val pairs = sentence_alignments(i).map(x=>(x._1, x._2+other_pairs(x._1._2,x._1._1))) //add in opposite order
             
             pairs.keys.toArray.sortWith(_._1 < _._1).foreach(x=>
               {if(pairs(x) == 2)mw.write(" %s-%s".format(x._1,x._2))
               //else mw.write(" %s-%s".format(x._1,x._2))
               })
             mw.write("\n")
          }
          mw.close()
        
      }
      
      
      
      
      
      
      
      def alignments(f:String){
          val mw = new BufferedWriter(new FileWriter(new File(f)))
          val r = (0 to source.size-1)
          for(i <- r){
             val source_sentence = source(i)
             val target_sentence = target(i)
             var s = ""
             var pairs = Map[Tuple2[Int,Int],Int]().withDefaultValue(0)
             for(x<- 0 to source_sentence.size-1){
                val al = (0 to target_sentence.size-1).map(y=>(trans(source_sentence(x),target_sentence(y)),x,y)  ).maxBy(x=>x._1)
                if(target_array(target_sentence(al._3))!=null_value){
                  mw.write("%s-%s ".format(al._2, al._3))
                }
             }
             mw.write("\n")
          }
          mw.close()
      }
      
     
      
      
      
}


object run_ibm extends App{
  
  override def main(args: Array[String]){
    
    val start = System.currentTimeMillis()
    
    val fs = Source.fromFile(args(0))
    val ft = Source.fromFile(args(1))
    val source = fs.getLines().map(x=>x.stripLineEnd.split("\\s+")).toArray
    val target = ft.getLines().map(x=>x.stripLineEnd.split("\\s+")).toArray
    fs.close();
    ft.close();
    println("read files")
    val sourceT = new IBM1(source, target)
    sourceT.train(args(2).toInt)
    val targetS = new IBM1(target, source)
    targetS.train(args(2).toInt)
    sourceT.dual_alignments(args(3), targetS)
    val end = System.currentTimeMillis()
    println("Total running time: %s seconds".format((end-start)/1000))
  }
  
}