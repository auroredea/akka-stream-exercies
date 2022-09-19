package part4_techniques

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}

import java.util.Date
import scala.concurrent.Future

object Integrations extends App {
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

  object PagerService {
    private val engs = List("Daniel", "John", "Aurore")
    private val emails = Map(
      "Daniel" -> "daniel@rockthejvm.com",
      "John" -> "john@doe.com",
      "Aurore" -> "aurore@home.com"
    )

    def processEvent(pagerEvent: PagerEvent) = Future {
      val engineerIndex = (pagerEvent.date.toInstant.getEpochSecond / (24 * 3600)) % engs.length
      val eng = engs(engineerIndex.toInt)
      val email = emails(eng)

      println(s"sending to $eng an email at $email : $pagerEvent")
      Thread.sleep(1000)
      email
    }
  }

  val akkaEvents = eventSource.filter(_.app == "Akka")
  // mapAsync guarantees the relative order of elements, otherwise mapAsyncUnordered (faster)
  val pagedEngEmails = akkaEvents.mapAsync(parallelism = 1)(event => PagerService.processEvent(event))

  val sink = Sink.foreach[String](email => println(s"send notif to $email"))
  pagedEngEmails.to(sink).run()
}
