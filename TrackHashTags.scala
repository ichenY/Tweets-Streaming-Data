import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.twitter.TwitterUtils

object TrackHashTags{
  def main(args: Array[String]): Unit = {
    val consumerKey = System.setProperty("twitter4j.oauth.consumerKey", args(0))
    val consumerSecret = System.setProperty("twitter4j.oauth.consumerSecret", args(1))
    val accessToken = System.setProperty("twitter4j.oauth.accessToken", args(2))
    val accessTokenSecret = System.setProperty("twitter4j.oauth.accessTokenSecret", args(3))
    /*------auth------
          */

    val sparkConf = new SparkConf().setAppName("hw5-1").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("ERROR")
    val ssc = new StreamingContext(sc, Seconds(2))
    val stream = TwitterUtils.createStream(ssc, None)

    //stream.saveAsTextFiles("tweets", "txt")

    val hashTags = stream.flatMap(x => x.getText.split("\\s+").filter(_.startsWith("#")))
    val top5 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(120)).map { case (topic, count) => (count, topic) }.transform(_.sortByKey(false))//.map { case (topic, count) => (count, topic) }//.transform(_.sortByKey(false))

    top5.foreachRDD(rdd => {
      val count1 = rdd.count()
      if (count1 > 0) {
        val topList = rdd.take(5)
        println("\nTop5 Hashtags:")
        topList.foreach { case (count, tag) => println("%s , count : %s".format(tag, count)) }}
    })


    ssc.start()
    ssc.awaitTermination()
    ssc.stop(true)
    sc.stop()



  }


}


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

import java.util.concurrent.atomic.AtomicLong
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter._

object AveTweetLength {
  def main(args: Array[String]): Unit = {
    val consumerKey = System.setProperty("twitter4j.oauth.consumerKey", args(0))
    val consumerSecret = System.setProperty("twitter4j.oauth.consumerSecret", args(1))
    val accessToken = System.setProperty("twitter4j.oauth.accessToken", args(2))
    val accessTokenSecret = System.setProperty("twitter4j.oauth.accessTokenSecret", args(3))
    /*----------oath---------
          */

    val sparkConf = new SparkConf().setAppName("hw5-1-2").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("ERROR")
    val ssc = new StreamingContext(sc, Seconds(2))
    val stream = TwitterUtils.createStream(ssc, None)

    val statuses = stream.map(_.getText)
    val lengths = statuses.map(_.length)

    val totalTweets = new AtomicLong(0)
    val totalChars = new AtomicLong(0)


    lengths.foreachRDD((rdd, _) => {

      val count = rdd.count()
      if (count > 0) {
        totalTweets.getAndAdd(count)

        totalChars.getAndAdd(rdd.reduce(_ + _))

        println(f"Total tweets:${totalTweets.get()}%7d  Average length:  ${totalChars.get() / totalTweets.get()}%7d ")
      }
    })





    ssc.start()
    ssc.awaitTermination()
    ssc.stop(true)
    sc.stop()
  }
}

import java.io.{PrintWriter}

import java.net.ServerSocket

import scala.io.Source



object StreamingSimulation {

  def main(args: Array[String]) {

    // Three args: directory of file, port #, time interval(millisecond)

    if (args.length != 3) {

      System.err.println("Usage: <filename> <port> <millisecond>")

      System.exit(1)

    }


    val filename = args(0)

    val lines = Source.fromFile(filename).getLines.toList

    val filerow = lines.length


    val listener = new ServerSocket(args(1).toInt)

    while (true) {

      val socket = listener.accept()

      new Thread() {

        override def run = {

          println("Got client connected from: " + socket.getInetAddress)

          val out = new PrintWriter(socket.getOutputStream(), true)
          var start = 0

          while (start < filerow) {

            Thread.sleep(args(2).toLong)

            val content = lines(start)

            println(content)
            out.write(content + '\n')
            out.flush()

            start = start + 1

          }

          socket.close()

        }

      }.start()

    }

  }

}







