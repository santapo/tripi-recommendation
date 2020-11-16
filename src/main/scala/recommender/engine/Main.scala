package recommender.engine

import recommender.engine.core.{DataPreprocessing, DataPreprocessingNew, DataProcessing}
import recommender.engine.restserving.AkkaServer

object Main {
  def main(args: Array[String]): Unit = {

//    val getdata = new DataProcessing
//
//    getdata.mapping()

    val server = new AkkaServer
    server.start()

    // val server = new AkkaServer
    // server.start()

  }
}
