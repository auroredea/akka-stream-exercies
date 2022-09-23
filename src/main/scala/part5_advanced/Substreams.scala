package part5_advanced

import akka.actor.ActorSystem
import akka.stream.scaladsl.Source.actorRefWithAck
import akka.stream.scaladsl.{Keep, Sink, Source}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

object Substreams extends App {
  implicit val system = ActorSystem("substreams")

  // 1 - grouping stream by a certain function
  val wordSource = Source(List("Akka", "is", "amazing"))
  val groups = wordSource.groupBy(30, word => if (word.isEmpty) '\u0000' else word.toLowerCase().charAt(0))

  groups.to(Sink.fold(0)((count, word) => {
    val newCount = count + 1
    println(s"I received $word, count is $newCount")
    newCount
  }))
  //    .run()

  // 2 - merge substream back
  val textSource = Source(List(
    "I love Akka",
    "this is amazing",
    "learning from Rock the JVM"
  ))

  val totalCharCount = textSource.groupBy(2, string => string.length % 2)
    .map(_.length) //executed for each substream (so in parallel)
    .mergeSubstreamsWithParallelism(2) // number of cap, not the same as number of substream, can reach deadlock
    // .mergeSubstreams is better
    .toMat(Sink.reduce[Int](_ + _))(Keep.right)
    .run()

  totalCharCount.onComplete {
    case Success(value) => println(s"total char count $value")
    case Failure(_) => println("error")
  }

  // 3 - splitting a stream into a substream, when condition is met
  val text =
    "I love Akka\n" +
    "this is amazing\n" +
    "learning from Rock the JVM\n"

  val anotherCharCount = Source(text.toList)
    .splitWhen(c => c == '\n')
    .filter(_ != '\n')
    .map(_ => 1)
    .mergeSubstreams
    .toMat(Sink.reduce[Int](_ + _))(Keep.right)
    .run()

  anotherCharCount.onComplete {
    case Success(value) => println(s"total char count from another $value")
    case Failure(_) => println("error")
  }

  // 4 - flattening
  val simpleSource = Source(1 to 5)
  simpleSource.flatMapConcat(x => Source(x to (100 * x))).runWith(Sink.ignore)
  //another instance (not like dynamic stream)
  simpleSource.flatMapMerge(2, x => Source(x to (10 * x))).runWith(Sink.ignore)


  // substream of words
  val wordCount = textSource
    .groupBy(2, sentence => sentence.length % 2)
    .mapConcat(sentence => sentence.split(' '))
    .mergeSubstreams
    .toMat(Sink.fold(0)((count, _) => count + 1))(Keep.right)
    .run()

  wordCount.onComplete {
    case Success(value) => println(s"total word count $value")
    case Failure(_) => println("error")
  }


}
