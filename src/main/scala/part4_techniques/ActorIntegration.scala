package part4_techniques

import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import akka.stream.scaladsl.{Sink, Source}
import akka.util.Timeout
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime
import part4_techniques.Integrations.{eventSource, pagedEngEmails}

import java.util.Date
import scala.concurrent.Future

object ActorIntegration extends App {
  implicit val sys = ActorSystem("integration")
  implicit val dispatcher = sys.dispatchers.lookup("dedicated-dispatcher")

  def genericService[A, B](element: A): Future[B] = ???

  // simplified pagerduty
  case class PagerEvent(app: String, desc: String, date: Date)

  val eventSource = Source(List(
    PagerEvent("Akka", "WARN", new Date()),
    PagerEvent("PagerEvent", "WARN", new Date()),
    PagerEvent("DataFace", "WARN", new Date()),
    PagerEvent("Pipeline", "WARN", new Date()),
    PagerEvent("Akka", "ERROR", new Date()),
  ))

  class PagerActor extends Actor with ActorLogging {
    private val engs = List("Daniel", "John", "Aurore")
    private val emails = Map(
      "Daniel" -> "daniel@rockthejvm.com",
      "John" -> "john@doe.com",
      "Aurore" -> "aurore@home.com"
    )

    private def processEvent(pagerEvent: PagerEvent) = Future {
      val engineerIndex = (pagerEvent.date.toInstant.getEpochSecond / (24 * 3600)) % engs.length
      val eng = engs(engineerIndex.toInt)
      val email = emails(eng)

      log.info(s"sending to $eng an email at $email : $pagerEvent")
      Thread.sleep(1000)
      email
    }

    override def receive: Receive = {
      case pagerEvent: PagerEvent => sender() ! processEvent(pagerEvent)
    }
  }

  import akka.pattern.ask
  implicit val timeout = Timeout(2.seconds)
  val akkaEvents = eventSource.filter(_.app == "Akka")
  val pagerActor = sys.actorOf(Props[PagerActor], "pager")
  val pagedEngEmails = akkaEvents.mapAsync(parallelism = 4)(event => (pagerActor ? event).mapTo[String])

  val sink = Sink.foreach[String](email => println(s"send notif to $email"))
  pagedEngEmails.to(sink).run()
}
