package recommender.engine

import recommender.engine.core.{DataPreprocessing, DataPreprocessingNew, DataProcessing}

object Main {
  def main(args: Array[String]): Unit = {

    val getdata = new DataPreprocessing

    getdata.dataFiltering()


    // val server = new AkkaServer
    // server.start()

  }
}
