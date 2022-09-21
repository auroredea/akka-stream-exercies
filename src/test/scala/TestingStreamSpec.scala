import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.testkit.{TestKit, TestProbe}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success}

class TestingStreamSpec extends TestKit(ActorSystem("testing"))
  with AnyWordSpecLike
  with BeforeAndAfterAll {

  override def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  "A simple stream" should {
    "satisfy basic assertions" in {
      val source = Source(1 to 10)
      val sink = Sink.fold[Int, Int](0)((a, b) => a + b)

      val sumFuture = source.toMat(sink)(Keep.right).run()
      val sum = Await.result(sumFuture, 2 second)
      assert(sum == 55, "sum")
    }

    "integrate with test actors via materialized values" in {
      import akka.pattern.pipe
      val source = Source(1 to 10)
      val sink = Sink.fold[Int, Int](0)((a, b) => a + b)

      val probe = TestProbe()

      source.toMat(sink)(Keep.right).run().pipeTo(probe.ref)

      probe.expectMsg(55)
    }

    "integrate with a test-actor-based sink" in {
      val source = Source(1 to 10)
      val flow = Flow[Int].scan[Int](0)(_ + _)
      val streamUnderTest = source.via(flow)

      val probe = TestProbe()
      val probeSink = Sink.actorRef(probe.ref, "good", {
        case _ => "an error occured"
      })

      streamUnderTest.to(probeSink).run()
      probe.expectMsgAllOf(0, 1, 3, 6, 10, 15)
    }

    "integrate with stream testkit sink" in {
      val sourceUnderTest = Source(1 to 5).map(_ * 2)
      val testSink = TestSink.probe[Int]

      val testValue = sourceUnderTest.runWith(testSink)
      testValue
        .request(5)
        .expectNext(2, 4, 6, 8, 10)
        .expectComplete()
    }

    "integrate with Streams TestKit Source" in {
      val sink = Sink.foreach[Int] {
        case 13 => throw new RuntimeException("bad luck")
        case _ =>
      }

      val testSource = TestSource.probe[Int]
      val mat = testSource.toMat(sink)(Keep.both)
        .run()
      val (testPub, resultFuture) = mat

      testPub
        .sendNext(1)
        .sendNext(5)
        .sendNext(13)
        .sendComplete()

      resultFuture.onComplete {
        case Success(_) => fail("have to fail at 13")
        case Failure(_) => // OK
      }
    }

    "test flows with a test source AND a test sink" in {
      val flow = Flow[Int].map(_ * 2)
      val source = TestSource.probe[Int]
      val sink = TestSink.probe[Int]

      val mat = source.via(flow).toMat(sink)(Keep.both)
        .run()

      val (pub, sub) = mat

      pub
        .sendNext(1)
        .sendNext(5)
        .sendNext(42)
        .sendNext(99)
        .sendComplete()

      sub.request(4)
        .expectNext(2, 10, 84, 198)
        .expectComplete()
    }
  }
}
