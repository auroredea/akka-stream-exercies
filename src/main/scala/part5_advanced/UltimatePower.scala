package part5_advanced

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{Attributes, Inlet, Outlet, SinkShape, SourceShape}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}

import scala.collection.mutable
import scala.util.Random

object UltimatePower extends App {
  implicit val system = ActorSystem("custom_operators")

  // 1 - source which emits random numbers until canceled

  class RandomNumberGenerator(max: Int = Int.MaxValue) extends GraphStage[SourceShape[Int]] {
    val outPort = Outlet[Int]("random")
    val random = new Random()

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
      //implement my logic here
      setHandler(outPort, new OutHandler {
        // when there is demand from downstream
        override def onPull(): Unit = {
          //emit a new element
          val nextNumber = random.nextInt(max)
          //push it to outPort
          push(outPort, nextNumber)
        }
      })
    }

    override def shape: SourceShape[Int] = SourceShape(outPort)
  }

  val randomGeneratorSource = Source.fromGraph(new RandomNumberGenerator(100))
//  randomGeneratorSource.runWith(Sink.foreach[Int](println))

  // 2 - a custom sink that prints elements in batches of a given size
  class Batcher(batchSize: Int) extends GraphStage[SinkShape[Int]] {
    val inPort = Inlet[Int]("batcher")

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
      //mutable state
      val batch = new mutable.Queue[Int]

      override def preStart(): Unit = {
        //ask first for demand upstream
        pull(inPort)
      }

      setHandler(inPort, new InHandler {
        //when upstream wants to send me an element
        override def onPush(): Unit = {
          val newtElement = grab(inPort)
          batch.enqueue(newtElement)
          //backpressure example : assume complex computation, so it will create automatically a backpressure signal
          // on upstream
          Thread.sleep(100)

          if(batch.size >= batchSize) println(s"batch : " + batch.dequeueAll(_ => true).mkString("[", ", ", "]"))
          pull(inPort) // send demand upstream
        }

        override def onUpstreamFinish(): Unit = {
          if(batch.nonEmpty) {
            println(s"batch : " + batch.dequeueAll(_ => true).mkString("[", ", ", "]"))
            println("finished")
          }
        }
      })
    }

    override def shape: SinkShape[Int] = SinkShape[Int](inPort)
  }

  val batcherSink = Sink.fromGraph(new Batcher(10))
  randomGeneratorSource.to(batcherSink).run()
}
