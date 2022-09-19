package part3_graphs

import akka.actor.ActorSystem
import akka.stream.{BidiShape, ClosedShape}
import akka.stream.scaladsl.{Flow, GraphDSL, RunnableGraph, Sink, Source}

object BidirectionalFlow extends App {
  implicit val system = ActorSystem("BidirectionalFlow")

  def encrypt(n: Int)(s: String) = s.map(c => (c + n).toChar)
  def decrypt(n: Int)(s: String) = s.map(c => (c - n).toChar)

  val biDiGraph = GraphDSL.create() { implicit builder =>
    val encFlowShape = builder.add(Flow[String].map(encrypt(3)))
    val decFlowShape = builder.add(Flow[String].map(decrypt(3)))

    BidiShape.fromFlows(encFlowShape, decFlowShape)
  }

  val list = List("Akka", "rocks", "baby")
  val source = Source(list)
  val encSource = Source(list.map(encrypt(3)))

  val cryptoGraph = RunnableGraph.fromGraph(
    GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      val unencryptedSourceShape = builder.add(source)
      val encryptedSourceShape = builder.add(encSource)
      val bidiShape = builder.add(biDiGraph)

      def printer[A](elType: String) = Sink.foreach[A](element => println(s"$elType: $element"))

      val encSinkShape = builder.add(printer("encrypted"))
      val decSinkShape = builder.add(printer("decrypted"))

      unencryptedSourceShape ~> bidiShape.in1  ;  bidiShape.out1 ~> encSinkShape
      // verifying encrypted string on bidiGraph are actually good
      decSinkShape           <~ bidiShape.out2 ;  bidiShape.in2  <~ encryptedSourceShape

      ClosedShape
    }
  )

  cryptoGraph.run()
}
