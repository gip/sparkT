package sparkt.backend.server

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives
import akka.stream.ActorMaterializer
import sparkt.backend.services.{SparkTService}
import scala.io.StdIn

object Server extends App {

  import Directives._

  implicit val actorSystem = ActorSystem("akka-system")
  implicit val flowMaterializer = ActorMaterializer()

  val config = actorSystem.settings.config
  val interface = config.getString("app.interface")

  val port = config.getInt("app.port")

  val route = SparkTService.route

  val binding = Http().bindAndHandle(route, interface, port)
  val banner = """ ____                   _    _____
/ ___| _ __   __ _ _ __| | _|_   _|
\___ \| '_ \ / _` | '__| |/ / | |
 ___) | |_) | (_| | |  |   <  | |
|____/| .__/ \__,_|_|  |_|\_\ |_|
      |_|"""
  println(Console.GREEN + banner + Console.WHITE)
  println(s"SparkT server is now online at http://$interface:$port\nPress RETURN to stop...")
  StdIn.readLine()

  import actorSystem.dispatcher

  binding.flatMap(_.unbind()).onComplete(_ => actorSystem.shutdown())
  println("SparkT server is down...")

}
