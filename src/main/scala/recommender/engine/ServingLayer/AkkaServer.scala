package recommender.engine.ServingLayer

import akka.actor.ActorSystem
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import scala.io.StdIn
import scala.concurrent.Future


object AkkaServer {

  // To run the route
  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()
  // Used for Future flatMap/onComplete/Done
  implicit val executionContext = system.dispatcher

  def start() {

    val route: Route = concat(
      get {
        path("search_query") {
          complete {
          }
        }
      }
    )

    // Binding to the host and port
    val bindingFuture = Http().bindAndHandle(route, "localhost", 8080)
  }


}
