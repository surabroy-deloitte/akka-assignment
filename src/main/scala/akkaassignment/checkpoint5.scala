package akkaassignment

import akka.NotUsed
import akka.actor.{ActorSystem, Props}
import akka.stream.{IOResult}
import akka.stream.scaladsl._
import akka.util.ByteString
import com.typesafe.config.ConfigFactory
import java.nio.file.Paths
import scala.concurrent.Future





object checkpoint5 extends App {


  val applicationConf = ConfigFactory.load("application.conf") //loading from configuration file
  val filepath = applicationConf.getString("app.filepath") //getting file path from configuration file

  implicit val system = ActorSystem("FileReaderSystem")

  val categoryFilter = applicationConf.getString("app.categoryFilter") //getting category filter from configuration file
  val categorySinkFile = applicationConf.getString("app.categorySinkFile") //getting category sink file from configuration file

  val masterActor = system.actorOf(Props[MasterActor], name = "masterActor")

  val fileSource: Source[ByteString, Future[IOResult]] =
    FileIO.fromPath(Paths.get(filepath))


  val categoryFilterFlow: Flow[ByteString, ByteString, NotUsed] =
    Flow[ByteString]
      .via(Framing.delimiter(ByteString("\n"), maximumFrameLength = 256, allowTruncation = true))
      .filter(record => {
        val fields = record.utf8String.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)").map(_.trim)
        fields(9) == categoryFilter
      })

  val categorySink: Sink[ByteString, Future[IOResult]] =
    FileIO.toPath(Paths.get(categorySinkFile))

  fileSource
    .via(categoryFilterFlow)
    .to(categorySink)
    .run()
}

//class MasterActor extends Actor {
//  // router for load balancing the work among the child actors
//  val router = context.actorOf(RoundRobinPool(10).props(Props[ChildActor]), "router")
//
//  // Watching the child actors for termination
//  context.watch(router)
//
//  def receive = {
//    case record: Array[String] =>
//      router ! record
//    case Terminated(actorRef) if actorRef == router =>
//      println("All child actors terminated.")
//      context.system.terminate()
//  }
//}

//class ChildActor extends Actor {
//  def receive = {
//    case record: Array[String] =>
//      println(record.mkString("\t"))
//  }
//}
