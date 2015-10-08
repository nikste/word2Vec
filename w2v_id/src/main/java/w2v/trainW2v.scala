package w2v

import org.apache.flink.api.common.functions.{FlatMapFunction, MapFunction}
import org.apache.flink.ml.feature_extraction.Word2Vec
import org.apache.flink.api.scala.{ExecutionEnvironment, DataSet}
;
import org.apache.flink.runtime.execution.Environment
import org.apache.flink.util.Collector
import twitter4j.{HashtagEntity, Status}
import twitter4j.TwitterObjectFactory._
import org.apache.flink.api.scala._
/**
 * Created by nikste on 08.10.15.
 */
object trainW2v {
  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    val inputFilePath = "/home/nikste/workspace-sd4m/dataset/german_tweets_100000_lines.json";

    val rawJson: DataSet[String] = env.readTextFile(inputFilePath)
    // load json file with flink
    val twitterStati: DataSet[Status] = rawJson.map{in => createStatus(in)};

    twitterStati.print()

    val twitterStatiFiltered: DataSet[Status] = twitterStati.filter(_.getHashtagEntities.length > 0)

    val twitterSentences: DataSet[Array[String]] = twitterStatiFiltered.map{new ConvertToArrayMapFunction}

    //twitterTrainInstances.print();
    //val inputW2v: DataSet[Array[String]] = twitterTrainInstances.map(new Job.TwitterTrainInstanceFlattener)

    trainW2v(twitterSentences)

    println("done!")
  }

  def trainW2v(input: DataSet[Array[String]]): Unit ={

    val w2v: Word2Vec = new Word2Vec
    w2v.setNumIterations(100)
    w2v.setBatchSize(1)
    w2v.setVectorSize(2)
    w2v.fit(input)

    Word2Vec.saveModel("/home/nikste/workspace-sd4m/datasets/w2v/tweetmodel")

  }

/*  class ConvertToTwitterTrainInstanceFlatMap extends org.apache.flink.api.common.functions.FlatMapFunction[Status, TwitterTrainInstance] {
    @throws(classOf[Exception])
    def flatMap(status: Status, collector: Collector[TwitterTrainInstance]) {
      val hashtagEntities: Array[HashtagEntity] = status.getHashtagEntities
      val text: Array[String] = status.getText.split(" ")
      val hashtags: Array[String] = Array.fill(hashtagEntities.length)
      {
        var i: Int = 0
        while (i < hashtagEntities.length) {
          {
            hashtags(i) = hashtagEntities(i).getText
          }
          ({
            i += 1; i - 1
          })
        }
      }
      if (!("" == text) && hashtagEntities.length > 0) {
        collector.collect(new TwitterTrainInstance(hashtags, text))
      }
    }
  }*/


  class ConvertToArrayMapFunction extends MapFunction[Status,Array[String]]{
    override def map(t: Status): Array[String] = {
      var hashs = t.getHashtagEntities
      var text = t.getText.split(" ")

      var totaltweets = Array.fill(hashs.length + text.length){""}

      var offset = hashs.length

      for(i <- 0 to hashs.length - 1){
        totaltweets(i) = hashs(i).getText
      }
      for(i <- 0  to text.length - 1){
        totaltweets(i + offset) = text(i)
      }
      totaltweets
    }
  }

}
