import java.util.concurrent.atomic.AtomicLong
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter._
import java.io._
import util.control.Breaks._

object UniqueUserCount {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("hw5-2").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("ERROR")
    val ssc = new StreamingContext(sc, Seconds(1))
    val lines= ssc.socketTextStream(args(0), args(1).toInt)//"localhost", 9999
    val hash_binary = lines.map(x => trailzero(x.toInt.toBinaryString))

    //val trail_zero=hash_binary.map(x=>Integer.numberOfTrailingZeros(x.toInt))
    var premx = -1
    hash_binary.foreachRDD(rdd=> {
      val count = rdd.count()
      var a = rdd.collect()
      var mx = 0
      if (count > 0) {
        mx = rdd.max()
      }

      if(mx>premx){
        println("Estimated users : "+Math.pow(2,mx).toInt)
        premx=mx
      }

   })

    //wordCounts.print()
    //reverse for loop
    ssc.start()
    ssc.awaitTermination()
    ssc.stop(true)
    sc.stop()



  }
  def trailzero(binaryS: String):Int= {
    val number = binaryS.reverse
    var counter = 0
    breakable{ for (i <- number) {
      if (i == '0')
        counter = counter + 1
      else break
    }}
  counter
  }

}

