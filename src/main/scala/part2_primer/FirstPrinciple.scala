package part2_primer

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, Sink, Source}

object FirstPrinciple extends App {
  implicit val system = ActorSystem("FirstPrinciple")

  val source = Source(1 to 10)
  val sink = Sink.foreach[Int](println)

  val graph = source.to(sink)
  // graph.run

  val flow = Flow[Int].map(x => x + 1)
  val sourceWith = source.via(flow)

  val anotherGraph = sourceWith.to(sink)
  // anotherGraph.run

  /* a stream of names and only keep the first 2 with length > 5 chars, print it */

  val nameSource = Source(List("Dupont", "Tuong", "De Amaral", "Lamodière")).filter(name => name.length > 5)
  nameSource.take(2).runForeach(println)
}
