package akkaassignment

import akka.actor.Actor
import akka.actor.Actor.Receive
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}

object checkpoint4 {
  class ErrorHandler extends Actor {
    implicit val materializer: ActorMaterializer = ActorMaterializer()

    override def receive: Receive = {
      case ex: Throwable =>
        // Log the error
        // ...
        Source.single(ex).runWith(Sink.foreach(println))
    }
  }
}
