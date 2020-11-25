package recommender.engine

import recommender.engine.core.{DataPreprocessing, DataPreprocessingNew, DataProcessingNew}
import recommender.engine.restserving.AkkaServer

object Main {
  def main(args: Array[String]): Unit = {

    val getdata = new DataProcessingNew

    getdata.dataClustering()

//    val data = new DataPreprocessingNew
//
//    data.dataToCassandra()

//    val data = new DataPreprocessing
//    data.dataFiltering()
//
//    val server = new AkkaServer
//    server.start()

  }
}
