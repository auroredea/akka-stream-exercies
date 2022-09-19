package part3_graphs

import akka.actor.ActorSystem
import akka.stream.{ClosedShape, UniformFanInShape}
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, MergePreferred, RunnableGraph, Sink, Source, Zip}

object Fibonacci extends App {
  implicit val sys = ActorSystem("fibonacci")

  // create fan-in shape
  // - two input fed with EXACTLY ONE number
  // - output will emit an INFINITE FIBONACCI SEQUENCE of these 2 numbers
  // 1, 2, 3, 5, 8...

  val fiboFlow = GraphDSL.create() { implicit builder =>
    import GraphDSL.Implicits._

    val zip = builder.add(Zip[BigInt, BigInt])
    val mergePreferred = builder.add(MergePreferred[(BigInt, BigInt)](1))
    val fiboLogic = builder.add(Flow[(BigInt, BigInt)].map(pair => {
      Thread.sleep(100)
      (pair._1 + pair._2, pair._1)
    }))
    val broadcast = builder.add(Broadcast[(BigInt, BigInt)](2))
    val extractLast = builder.add(Flow[(BigInt, BigInt)].map(_._1))

    zip.out ~> mergePreferred ~> fiboLogic ~> broadcast ~> extractLast
    mergePreferred.preferred <~ broadcast

    UniformFanInShape(extractLast.out, zip.in0, zip.in1)
  }

  val fiboGraph = RunnableGraph.fromGraph(
    GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      val source0 = builder.add(Source.single[BigInt](1))
      val source1 = builder.add(Source.single[BigInt](1))
      val printer = builder.add(Sink.foreach[BigInt](println))
      val fibo = builder.add(fiboFlow)

      source0 ~> fibo.in(0)
      source1 ~> fibo.in(1)

      fibo.out ~> printer

      ClosedShape
    }
  )
  fiboGraph.run()
}
