package recommender.engine


import recommender.engine.ServingLayer.AkkaServer
import akka.actor.{ActorSystem, Props}
import recommender.engine.DataProcessing.DataPreprocessing

import scala.concurrent.duration.DurationInt

object Main {
  def main(args: Array[String]): Unit = {

    val getdata = new DataPreprocessing

    getdata.dataFiltering()

    
    // val server = new AkkaServer
    // server.start()

  }
}
