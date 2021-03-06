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
        } ~ path("lowest_price") {
          parameters('page.as[Int], 'key.as[String]) { (page, key) =>
            complete {
              val hotel_table = readData.readData()
              val getprice = hotel_table.lowest_price(page, key).load()
              getprice
            }
          }
        } ~ path("top_province") {
          parameters('page.as[Int]) { (page) =>
            complete {
              val hotel_table = readData.readData()
              val getprice = hotel_table.top_province(page).load_province()
              getprice
            }
          }
        }
      }
    )

    // Binding to the host and port
    val bindingFuture = Http().bindAndHandle(route, "0.0.0.0", 3800)
    println(Calendar.getInstance().getTime + s": Server online at http://13.212.124.210:3800\nPress Enter to stop...\n")
    StdIn.readLine() // let the server run until user presses Enter

    bindingFuture
      .flatMap(_.unbind()) // trigger unbinding from the port
      .onComplete(_ ⇒ system.terminate()) // and shutdown when done
  }
}