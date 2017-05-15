import java.io.File

import org.apache.spark._
import org.apache.spark.storage._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter.TwitterUtils
import com.typesafe.config.{Config, ConfigFactory}

object SparkStreaming {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("TwitterStreamSentiment")
    
    val sc = new SparkContext("local[2]", "test", conf)

    val config = ConfigFactory.load()
    val ssc = new StreamingContext(sc, Seconds(5))
    
    System.setProperty("twitter4j.oauth.consumerKey", config.getString("twitter4j.oauth.consumerKey"))
    System.setProperty("twitter4j.oauth.consumerSecret", config.getString("twitter4j.oauth.consumerSecret"))
    System.setProperty("twitter4j.oauth.accessToken", config.getString("twitter4j.oauth.accessToken"))
    System.setProperty("twitter4j.oauth.accessTokenSecret", config.getString("twitter4j.oauth.accessTokenSecret"))

    val stream = TwitterUtils.createStream(ssc, None)
    stream.print()
    //val tags = stream.flatMap(status => status.getHashtagEntities.map(_.getText))
    
    //tags.print

    ssc.start()
    ssc.awaitTerminationOrTimeout(20000)
  }
}
