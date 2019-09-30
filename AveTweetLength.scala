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
    /*----oauth---
          */

    val sparkConf = new SparkConf().setAppName("hw5-1-2").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("ERROR")
    val ssc = new StreamingContext(sc, Seconds(2))
    val stream = TwitterUtils.createStream(ssc, None)
    val totalTweets = new AtomicLong(0)
    val totalChars = new AtomicLong(0)
    val statuses = stream.map(_.getText)
    val lengths = statuses.map(_.length)

   


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
