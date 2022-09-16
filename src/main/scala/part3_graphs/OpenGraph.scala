package part3_graphs

import akka.actor.ActorSystem
import akka.stream.{FlowShape, SourceShape}
import akka.stream.scaladsl.{Broadcast, Concat, Flow, GraphDSL, Merge, Sink, Source}

object OpenGraph extends App {
  implicit val system = ActorSystem("opengraph")

  val fSource = Source(1 to 10)
  val sSource = Source(42 to 100)
  val sinkPrint = Sink.foreach(println)

  val sourceGraph = Source.fromGraph(
    GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      val concat = builder.add(Concat[Int](2))

      fSource ~> concat
      sSource ~> concat
      SourceShape(concat.out)
    }
  )

  // Complex source (same for sink with SinkShape)
  // sourceGraph.to(sinkPrint).run()

  /* flow composed of two flows, one add 1 o number, other * 10 */

  val flowAdd = Flow[Int].map(x => x + 1)
  val flowMul = Flow[Int].map(x => x * 10)

  val flowGraph = Flow.fromGraph(
    GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      val incrShape = builder.add(flowAdd)
      val mulShape = builder.add(flowMul)

      incrShape ~> mulShape

      FlowShape(incrShape.in, mulShape.out)
    }
  )

  sourceGraph.via(flowGraph).to(sinkPrint).run()
}
