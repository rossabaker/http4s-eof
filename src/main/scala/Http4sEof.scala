package io.bitrise.apm.symbolicator

import cats.effect.{ExitCode, IO, IOApp}
import fs2.Stream
import org.http4s.{HttpRoutes, _}
import org.http4s.client.Client
import org.http4s.client.blaze.BlazeClientBuilder
import org.http4s.dsl.io._
import org.http4s.implicits._
import org.http4s.server.blaze.BlazeServerBuilder

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

object Http4sEof extends IOApp {

  // Numbers below vary on different computers
  // In my case, if the request payload size is 32603 or greater
  //  AND response payload size is 81161 or greater
  //  then I get an EOF exception in some but not all cases
  // If however either of these payload sizes is lower then
  //  EOF exception doesn't occur, even if running for an extended period

  // broken on first request
  val appTime = 5.seconds
  val requestPayloadSize = 65398
  val responsePayloadSize = 81161

  // can hold for 5 seconds, but broken on longer run, like 30 seconds appTime
  //val appTime = 30.seconds
  //val requestPayloadSize = 65397
  //val responsePayloadSize = 81161

  // more stable, can hold up to 30 seconds appTime, seen broken on 120 seconds
  //val appTime = 30.seconds
  //val requestPayloadSize = 65398
  //val responsePayloadSize = 81160

  val uri = uri"http://localhost:8099"
  val body = "x" * requestPayloadSize
  val req = Request[IO](POST, uri).withEntity(body)
  val response = "x" * responsePayloadSize

  var i = 0
  override def run(args: List[String]): IO[ExitCode] = {
    def requestStream(client: Client[IO]): Stream[IO, Unit] = Stream
      .fixedRate(0.01.second)
      .flatMap(_ => {
        i = i + 1

        client.stream(req).flatMap(_.bodyText)
      })
      .evalMap(c => IO.delay(println(s"$i ${c.size}")))
      .interruptAfter(appTime)

    server(simpleClient.stream.flatMap(requestStream))
  }

  val simpleClient: BlazeClientBuilder[IO] =
    BlazeClientBuilder[IO](ExecutionContext.global)
      .withRequestTimeout(45.seconds)
      .withIdleTimeout(1.minute)
      .withResponseHeaderTimeout(44.seconds)

  def server(app: Stream[IO, Unit]) =
    BlazeServerBuilder[IO](ExecutionContext.global)
      .withIdleTimeout(5.minutes)
      .bindHttp(8099, "0.0.0.0")
      .withHttpApp(
        HttpRoutes
          .of[IO] {
            case POST -> Root => Ok(response)
          }
          .orNotFound
      )
      .serve
      .concurrently(app)
      .interruptAfter(appTime + 2.seconds)
      .compile
      .drain
      .as(ExitCode.Success)

}
