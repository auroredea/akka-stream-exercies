package part3_graphs

import akka.actor.ActorSystem
import akka.stream.{FlowShape, SinkShape}
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Keep, Sink, Source}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}

object Materialized extends App {
  implicit val system = ActorSystem("materializedGraph")

  val wordSource = Source(List("Akka", "is", "close", "source"))
  val printer = Sink.foreach(println)

  // future[Int] materialized value of the Sink
  val counter = Sink.fold[Int, String](0)((count, _) => count + 1)

  // composite Sink component, print lowercase and count shorts (<5chars) strings
  val wordSink = Sink.fromGraph(
    // createGraph for materialized values !!
    GraphDSL.createGraph(counter) { implicit builder => counterShape =>
      import GraphDSL.Implicits._

      val broadcast = builder.add(Broadcast[String](2))
      val lowercase = builder.add(Flow[String].filter(word => word == word.toLowerCase()))
      val shorts = builder.add(Flow[String].filter(_.length < 5))

      broadcast ~> lowercase ~> printer
      broadcast ~> shorts ~> counterShape

      SinkShape(broadcast.in)
    }
  )

  val future = wordSource.toMat(wordSink)(Keep.right).run()
  future.onComplete {
    case Success(value) => println(s"Total number of short strings is : ${value + 1}")
    case Failure(exception) => println(s"Error : ${exception.getMessage}")
  }

  /* exercice */
  def enhanceFlow[A, B](flow: Flow[A, B, _]): Flow[A, B, Future[Int]] = {
    val counterSink = Sink.fold[Int, B](0)((count, _) => count + 1)

    Flow.fromGraph(
      GraphDSL.createGraph(counterSink) { implicit builder => counterShape =>
        import GraphDSL.Implicits._
        val broadcast = builder.add(Broadcast[B](2))
        val originalFlowShape = builder.add(flow)

        originalFlowShape ~> broadcast ~> counterShape
        broadcast ~> counterShape

        FlowShape(originalFlowShape.in, broadcast.out(1))
      }
    )
  }

  val simpleSource = Source(1 to 42)
  val simpleFlow = Flow[Int].map(x => x)
  val simpleSink = Sink.ignore

  val enhancedFlowCount = simpleSource.viaMat(enhanceFlow(simpleFlow))(Keep.right).toMat(simpleSink)(Keep.left).run()
  enhancedFlowCount.onComplete {
    case Success(count) => println(s"$count !")
    case _ => println("failed :(")
  }
}
