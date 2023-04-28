package akkaassignment

import akka.actor.AbstractActor.Receive
import akka.actor.{Actor, ActorRef, ActorSystem, Props, Terminated}
import akka.routing.RoundRobinPool
import com.typesafe.config.ConfigFactory

import java.io.File
import java.util.Date
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import akkaassignment.purchases.validation

import java.text.SimpleDateFormat
import java.time.{LocalDate, ZoneId}
import java.time.format.DateTimeFormatter
import scala.util.{Failure, Success, Try}
//checlpoint1-4
case class Purchase(orderdate: LocalDate,
                     shipdate: LocalDate,
                     shipmode: String,
                     customername: String,
                     segment: String,
                     country: String,
                     city: String,
                     state: String,
                     region: String,
                     category: String,
                     subcategory: String,
                     name: String,
//                     rounded:Double,
                     sales: Double,
                     quantity: Int,
                     discount: Double,
                     profit: Double
                    )
case class InvalidPurchaseRecord(record: Array[String], error: String)
object purchases extends App {
  import scala.io.Source
  val applicationConf = ConfigFactory.load("application.conf") //loading from configuration file
  val filepath = applicationConf.getString("app.filepath") //getting file path from configuration file

  val source=Source.fromFile(filepath)
  val lines=source.getLines()
  val system=ActorSystem("FileReaderSystem")
  val fileReaderActor=system.actorOf(Props[MasterActor],name = "fileReaderActor")

  for(line<-lines){
    val record=line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)(?<! )(?<! )").map(_.trim)
    fileReaderActor ! record

  }
  source.close()
//  system.terminate()
def validation(record: Array[String]): Boolean = {
  for (i <- record) {
    if (i == "" || i == null) {
      return false
    }
  }
  return true
}

}

class MasterActor extends Actor {
  // router for load balancing the work among the child actors
  val router = context.actorOf(RoundRobinPool(10).props(Props[ChildActor]), "router")

  // Watching the child actors for termination
  context.watch(router)

  def receive = {
    case record: Array[String] =>
      router ! record
    case Terminated(actorRef) if actorRef == router =>
      println("All child actors terminated.")
      context.system.terminate()
  }
}


class ChildActor extends Actor {
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  override def receive: Receive = {
    case fields: Array[String] =>


//      val dateFormat = new SimpleDateFormat("MM/dd/yyyy")
      val dateFormat1 = DateTimeFormatter.ofPattern("MM/dd/yyyy")
      val dateFormat2 = DateTimeFormatter.ofPattern("M/d/yyyy")
      val dateFormat3 = DateTimeFormatter.ofPattern("M/dd/yyyy")
      val dateFormat4 = DateTimeFormatter.ofPattern("MM/d/yyyy")
      val result = Try(
        Try(LocalDate.parse(fields(0), dateFormat1)).orElse(Try(LocalDate.parse(fields(0), dateFormat2)))
          .orElse(Try(LocalDate.parse(fields(0), dateFormat3))).orElse(Try(LocalDate.parse(fields(0), dateFormat4))),
        Try(LocalDate.parse(fields(1), dateFormat1)).orElse(Try(LocalDate.parse(fields(1), dateFormat2)))
          .orElse(Try(LocalDate.parse(fields(1), dateFormat3))).orElse(Try(LocalDate.parse(fields(1), dateFormat4))),
      fields(2),
      fields(3),
      fields(4),
      fields(5),
      fields(6),
      fields(7),
      fields(8),
      fields(9),
      fields(10),
      fields(11),
        fields(12).toDouble,
        fields(13).toInt,
        fields(14).toDouble,
        fields(15).toDouble
    )
      result match {
        case Success(data) =>
          if(validation(fields)) {
            val source=Source.single(data)
            val sink=Sink.foreach(println)
            source.to(sink).run()
          }
        case Failure(ex) =>
          // Handle the validation error
          // ...
          context.actorSelection("/user/masterActor/errorHandler") ! ex
      }
  }
}


class ErrorHandler extends Actor {
  implicit val materializer: ActorMaterializer = ActorMaterializer()

  override def receive: Receive = {
    case ex: Throwable =>
      // Log the error
      // ...
      Source.single(ex).runWith(Sink.foreach(println))
  }

}