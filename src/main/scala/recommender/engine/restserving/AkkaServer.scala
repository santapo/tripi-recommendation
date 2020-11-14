package recommender.engine.restserving

import java.util.Calendar

import akka.actor.ActorSystem
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import recommender.engine.core.DataReader

import scala.io.StdIn
import scala.concurrent.Future


class AkkaServer {
  val readData = DataReader

  // To run the route
  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()
  // Used for Future flatMap/onComplete/Done
  implicit val executionContext = system.dispatcher

  def start() {

    val route: Route = concat(
      get {
        path("search_query") {
          parameters('page.as[Int],'key.as[String]) { (page, key) =>
            complete {
              val hotel_table = readData.readData()
              val getprice = hotel_table.search(page, key).load()
              getprice
            }
          }
        }
      }
    )

    // Binding to the host and port
    val bindingFuture = Http().bindAndHandle(route, "localhost", 8080)
    println(Calendar.getInstance().getTime + s": Server online at http://localhost:8080/\nPress Enter to stop...\n")
    StdIn.readLine() // let the server run until user presses Enter

    bindingFuture
      .flatMap(_.unbind()) // trigger unbinding from the port
      .onComplete(_ ⇒ system.terminate()) // and shutdown when done
  }
}
