package part3_graphs

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ClosedShape
import akka.stream.scaladsl.{Balance, Broadcast, Flow, GraphDSL, Merge, RunnableGraph, Sink, Source, Zip}

import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

object GraphBasics extends App {
  implicit val system = ActorSystem("graph")

  val input = Source(1 to 100)
  val incr = Flow[Int].map(_ + 1)
  val mul = Flow[Int].map(_ * 10)
  val out = Sink.foreach[(Int, Int)](println)

  val graph = RunnableGraph.fromGraph(
    GraphDSL.create() { implicit b: GraphDSL.Builder[NotUsed] =>
      import GraphDSL.Implicits._

      val broadcast = b.add(Broadcast[Int](2)) // fan-out operator
      val zip = b.add(Zip[Int, Int]) // fan-in operator

      // tying up source -> -> broadcast -> -> incr, mul -> -> zip -> out
      input ~> broadcast

      broadcast.out(0) ~> incr ~> zip.in0
      broadcast.out(1) ~> mul ~> zip.in1

      zip.out ~> out

      ClosedShape // shape
    } // graph
  ) // runnable graph

  // graph.run()

  /* feed a source into 2 sinks at the same time (hint: use a broadcast) */
  val source = Source(1 to 100)
  val sink1 = Sink.ignore
  val sink2 = Sink.ignore

  val exerciseGraph = RunnableGraph.fromGraph(
    GraphDSL.create() { implicit b2: GraphDSL.Builder[NotUsed] =>
      import GraphDSL.Implicits._

      val broadcast = b2.add(Broadcast[Int](2))

      input ~> broadcast
      broadcast.out(0) ~> sink1
      broadcast.out(1) ~> sink2

      ClosedShape
    }
  )

  // exerciseGraph.run()

  /* slow and fast source then a merge, then a balance to 2 sinks */
  val fastSource = Source(1 to 100)
  val slowSource = fastSource.throttle(2, 1 second)
  val firstSink = Sink.foreach[Int](x => println(s"sink 1 : $x"))
  val secondSink = Sink.foreach[Int](x => println(s"sink 2 : $x"))

  val complGraph = RunnableGraph.fromGraph(
    GraphDSL.create() { implicit b3: GraphDSL.Builder[NotUsed] =>
      import GraphDSL.Implicits._

      val merge = b3.add(Merge[Int](2))
      val balance = b3.add(Balance[Int](2))

      fastSource ~> merge ~> balance ~> firstSink
      slowSource ~> merge; balance ~> secondSink

      ClosedShape
    }
  )

  complGraph.run()
}