package akkaassignment

import akka.actor.Actor

import com.typesafe.config.ConfigFactory
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}


import java.text.SimpleDateFormat
import java.time.{LocalDate, ZoneId}
import java.time.format.DateTimeFormatter
import scala.util.{Failure, Success, Try}

object checkpoint3 {


  val applicationConf = ConfigFactory.load("application.conf");
  val categorysinkfile = applicationConf.getString("app.CategorySinkFile")

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
            if (validation(fields)) {
              val source = Source.single(data)
              val sink = Sink.foreach(println)
              source.to(sink).run()
            }
          case Failure(ex) =>
            // Handle the validation error
            // ...
            context.actorSelection("/user/masterActor/errorHandler") ! ex
        }
    }

    def validation(record: Array[String]): Boolean = {
      for (i <- record) {
        if (i == "" || i == null) {
          return false
        }
      }
      return true
    }

  }
}
