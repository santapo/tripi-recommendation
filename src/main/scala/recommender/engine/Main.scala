package recommender.engine

import recommender.engine.ServingLayer.AkkaServer

import akka.actor.{ActorSystem, Props}
import scala.concurrent.duration.DurationInt

object Main {
  def main(args: Array[String]): Unit = {


    AkkaServer.start()
  }
}
