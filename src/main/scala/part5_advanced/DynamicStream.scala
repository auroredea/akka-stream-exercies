package part5_advanced

import akka.actor.ActorSystem
import akka.stream.KillSwitches
import akka.stream.scaladsl.{Broadcast, BroadcastHub, Flow, Keep, MergeHub, Sink, Source}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

object DynamicStream extends App {
  implicit val system = ActorSystem("dynamic")

  // 1 - Kill switch
  val killSwitchFlow = KillSwitches.single[Int]
  val counter = Source(LazyList.from(1)).throttle(1, 1 second).log("counter")
  val sink = Sink.ignore

  val killSwitch = counter
    .viaMat(killSwitchFlow)(Keep.right)
    .to(sink)
//    .run()

  system.scheduler.scheduleOnce(3 seconds) {
    //killSwitch.shutdown()
  }

  val anotherCounter = Source(LazyList.from(1)).throttle(1, 1 second).log("counter 2")
  val sharedKillSwitch = KillSwitches.shared("oneButtonToRuleThemAll")

  counter.via(sharedKillSwitch.flow).runWith(Sink.ignore)
  anotherCounter.via(sharedKillSwitch.flow).runWith(Sink.ignore)

  system.scheduler.scheduleOnce(3 seconds) {
    sharedKillSwitch.shutdown()
  }

  // 2 - MergeHub -> stream inception
  val dynamicMerge = MergeHub.source[Int]
  val matSink = dynamicMerge.to(Sink.foreach[Int](println)).run()

  // use this sink anytime we like, unlike GraphDSL mutable creation
  Source(1 to 10).runWith(matSink)
  counter.runWith(matSink)

  // 3 - BroadcastHub -> stream inception bis
  val dynamicBroadcast = BroadcastHub.sink[Int]
  val matSource = Source(1 to 100).runWith(dynamicBroadcast)

  matSource.runWith(Sink.ignore)
  matSource.runWith(Sink.foreach(println))

  // challenge - combine mergehub & broadcasthub
  // publisher <-> subscriber component
  val flow = Flow[String].fold("")((previous, last) => previous + " " + last)

  val myDynamicBroadcast = BroadcastHub.sink[String]

  val myMergeHub = MergeHub.source[String]

  val (publisherPort, subscriberPort) = myMergeHub.toMat(myDynamicBroadcast)(Keep.both).run()
  subscriberPort.runWith(Sink.foreach(e => println(s"I received $e")))
  subscriberPort.map(s => s.length).runWith(Sink.foreach(n => println(s"I got number $n")))
}
