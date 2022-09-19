package part3_graphs

import akka.actor.ActorSystem
import akka.stream.{ClosedShape, OverflowStrategy, UniformFanInShape}
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Merge, MergePreferred, RunnableGraph, Sink, Source, Zip, ZipWith}

object GraphCycle extends App {
  implicit val system = ActorSystem("cycles")

  val deadlock = GraphDSL.create() { implicit builder =>
    import GraphDSL.Implicits._

    val source = builder.add(Source(1 to 1000))
    val merge = builder.add(Merge[Int](2))
    val incre = builder.add(Flow[Int].map { x =>
      println(s"Accelerating $x")
      x + 1
    })

    source ~> merge ~> incre
              merge <~ incre

    ClosedShape
  }

  // RunnableGraph.fromGraph(deadlock).run()
  // graph cycle deadlock!

  // Solution 1: MergePreferred
  val accelerator = GraphDSL.create() { implicit builder =>
    import GraphDSL.Implicits._

    val source = builder.add(Source(1 to 1000))
    val merge = builder.add(MergePreferred[Int](1))
    val incre = builder.add(Flow[Int].map { x =>
      println(s"Accelerating $x")
      x + 1
    })

    source ~> merge ~> incre
    merge.preferred <~ incre

    ClosedShape
  }
  //RunnableGraph.fromGraph(accelerator).run()

  // Solution 2 : buffering
  val bufferedRepeater = GraphDSL.create() { implicit builder =>
    import GraphDSL.Implicits._

    val source = builder.add(Source(1 to 1000))
    val merge = builder.add(Merge[Int](1))
    val repeater = builder.add(Flow[Int].buffer(10, OverflowStrategy.dropHead).map { x =>
      println(s"Accelerating $x")
      Thread.sleep(100)
      x + 1
    })

    source ~> merge ~> repeater
    merge <~ repeater

    ClosedShape
  }
  //RunnableGraph.fromGraph(bufferedRepeater).run()
}
