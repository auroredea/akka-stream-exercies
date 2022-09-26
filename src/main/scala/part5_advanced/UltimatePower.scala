package part5_advanced

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet, SinkShape, SourceShape}
import akka.stream.stage.{GraphStage, GraphStageLogic, GraphStageWithMaterializedValue, InHandler, OutHandler}

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Random, Success}

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
//  randomGeneratorSource.to(batcherSink).run()

  // Exercice : a custom flow - a simple filter flow
  // 2 ports : input and output
  class FilterFlow[I](filterPredicate: I => Boolean) extends GraphStage[FlowShape[I, I]] {
    val inPort = Inlet[I]("input filter flow")
    val outPort = Outlet[I]("output filter flow")
    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
      // when receive upstream
      setHandler(inPort, new InHandler {
        override def onPush(): Unit = {
          try {
            val nextElement = grab(inPort)
            if (filterPredicate(nextElement)) push(outPort, nextElement)
            else pull(inPort)
          } catch {
            case e: Throwable => failStage(e)
          }
        }
      })

      // when demand downstream
      setHandler(outPort, new OutHandler {
        override def onPull(): Unit = pull(inPort)
      })
    }

    override def shape: FlowShape[I, I] = FlowShape[I, I](inPort, outPort)
  }

  val filterFlow = Flow.fromGraph(new FilterFlow[Int](_ <= 10))
  //randomGeneratorSource.via(filterFlow).to(Sink.foreach(println)).run()

  // Materialized values in graph stages

  // 3 - a flow that counts the number of elements that go through it
  class CounterFlow[T] extends GraphStageWithMaterializedValue[FlowShape[T, T], Future[Int]] {
    val inPort = Inlet[T]("input counter flow")
    val outPort = Outlet[T]("output counter flow")

    override val shape = FlowShape(inPort, outPort)

    override def createLogicAndMaterializedValue(attributes: Attributes): (GraphStageLogic, Future[Int]) = {
      val promise = Promise[Int]
      val logic = new GraphStageLogic(shape) {
        // setting mutable state
        var counter = 0

        setHandler(outPort, new OutHandler {
          override def onPull(): Unit = pull(inPort)

          override def onDownstreamFinish(cause: Throwable): Unit = {
            promise.success(counter)
            super.onDownstreamFinish(cause)
          }
        })

        setHandler(inPort, new InHandler {
          override def onPush(): Unit = {
            val element = grab(inPort)
            counter += 1
            push(outPort, element)
          }

          override def onUpstreamFinish(): Unit = {
            promise.success(counter)
            super.onUpstreamFinish()
          }

          override def onUpstreamFailure(ex: Throwable): Unit = {
            promise.failure(ex)
            super.onUpstreamFailure(ex)
          }
        })
      }

      (logic, promise.future)
    }
  }

  val counterFlow = Flow.fromGraph(new CounterFlow[Int])
  val count = Source(1 to 10)
    // Test error
//    .map(e => if (e == 4) throw new RuntimeException("ERROR") else e)
    .viaMat(counterFlow)(Keep.right)
    // Cut test error
//    .to(Sink.foreach(e => if (e == 5) throw new RuntimeException("GOTCHA") else println(e)))
    .to(Sink.foreach(println))
    .run()

  count.onComplete {
    case Success(count) => println(s"count = $count")
    case Failure(exception) => println(s"counting error : $exception")
  }
}
