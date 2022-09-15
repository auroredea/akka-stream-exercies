package part2_primer

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

object Materialized extends App {
  implicit val actorSystem = ActorSystem("MaterializedStream")

  val simpleGraph = Source(1 to 10).to(Sink.foreach(println))
  // val simpleMaterializedValue = simpleGraph.run

  val source = Source(1 to 10)
  val sink = Sink.reduce[Int]((a, b) => a + b)
  val sumFuture = source.runWith(sink)
  sumFuture.onComplete {
    case Success(value) => println(s"The sum is $value")
    case Failure(_) => println("error")
  }

  // materialized stream
  val flow = Flow[Int].map(x => x * 2)
  val simpleSink = Sink.foreach[Int](println)
  val graph = source.viaMat(flow)(Keep.right).toMat(simpleSink)(Keep.right)
  graph.run().onComplete {
    case Success(_) => println("Stream finished")
    case Failure(exception) => s"Stream exception : ${exception.getMessage}"
  }

  /* return the last element out of a source (use Sink.last)
     compute the total word count out of a stream of sentences
     map, fold, reduce
   */

  val sentences = List("Une coupe", "Un joyau", "La terre entière")
  val sentencesSource = Source[String](sentences)

  sentencesSource.toMat(Sink.last)(Keep.right).run

  val sentenceGraph = sentencesSource.map[Int](sentence => sentence.count(_.isWhitespace) + 1).reduce[Int]((a, b) => a + b).runWith(Sink.head)
  sentenceGraph.onComplete {
    case Success(result) => println(s"Stream finished with total number of words $result")
    case Failure(exception) => s"Stream exception : ${exception.getMessage}"
  }
}
