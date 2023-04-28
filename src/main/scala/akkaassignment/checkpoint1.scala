package akkaassignment

import akka.actor.{ActorSystem, Props}
import akkaassignment.checkpoint2.ReaderActor
import com.typesafe.config.ConfigFactory

import java.time.LocalDate
import scala.io.Source

object checkpoint1 extends App {
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
                      sales: Double,
                      quantity: Int,
                      discount: Double,
                      profit: Double
                     )
  val config = ConfigFactory.load("application.conf")
  val filepath = config.getString("app.filepath")

  val source = Source.fromFile(filepath)
  val lines = source.getLines().drop(1)

  val system = ActorSystem("FileReader")
  val fileReaderActor = system.actorOf(Props[ReaderActor], "fileReaderActor")

  for (line <- lines) {
    val record = line.split(",")
    //    println(line.split(",").toList)
    fileReaderActor ! record

  }
}
