package akkaassignment

import checkpoint3.ChildActor
import checkpoint4.ErrorHandler
import akka.actor.{Actor, ActorRef, Props, Terminated}
import akka.routing.RoundRobinPool
import akka.actor.TypedActor.context

object checkpoint2 extends App {
  class ReaderActor extends Actor {

    val workerRouter: ActorRef = context.actorOf(RoundRobinPool(10).props(Props[ChildActor]), "workerRouter")
    val errorHandler: ActorRef = context.actorOf(Props[ErrorHandler], "errorHandler")

    override def receive: Receive = {
      case record: Array[String] =>
        workerRouter ! record
      case Terminated(child) =>
        println(s"Child actor ${child.path.name} terminated")

    }
  }
}
