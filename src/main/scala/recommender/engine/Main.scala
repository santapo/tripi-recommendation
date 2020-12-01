package recommender.engine

import recommender.engine.core.{DataPreprocessingNew, DataProcessingNew}
import recommender.engine.restserving.AkkaServer

object Main {
  def main(args: Array[String]): Unit = {

//    val getdata = new DataProcessingNew
//
//    getdata.dataMapping()
//    getdata.dataClustering()
//    getdata.dataRankScore()

//    val data = new DataProcessingNew
//
//    data.dataClustering()
//    data.dataRankScore()

    val data = new DataPreprocessingNew

    data.dataToCassandra()

//    val data = new DataPreprocessing
//    data.dataFiltering()
//
//    val server = new AkkaServer
//    server.start()

  }
}
