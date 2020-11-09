package recommender.engine


import recommender.engine.restserving.AkkaServer
import akka.actor.{ActorSystem, Props}
import recommender.engine.core.DataPreprocessing

import scala.concurrent.duration.DurationInt

object Main {
  def main(args: Array[String]): Unit = {

    val getdata = new DataPreprocessing

    getdata.dataFiltering()

    
    // val server = new AkkaServer
    // server.start()

  }
}
