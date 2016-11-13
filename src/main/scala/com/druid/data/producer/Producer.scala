package com.druid.data.producer


import com.metamx.tranquility.config.{DataSourceConfig, PropertiesBasedConfig, TranquilityConfig}
import com.metamx.tranquility.druid.DruidBeams
import com.metamx.tranquility.tranquilizer.{MessageDroppedException, Tranquilizer}
import com.twitter.util.{Return, Throw}
import org.joda.time.DateTime
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.util.Random


object Producer {

  val log = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]) {
    val configStream = getClass.getClassLoader.getResourceAsStream("example.json")
    val config: TranquilityConfig[PropertiesBasedConfig] = TranquilityConfig.read(configStream)
    val wikipediaConfig: DataSourceConfig[PropertiesBasedConfig] = config.getDataSource("wikipedia")
    val sender: Tranquilizer[java.util.Map[String, AnyRef]] =
      DruidBeams
        .fromConfig(config.getDataSource("wikipedia"))
        .buildTranquilizer(wikipediaConfig.tranquilizerBuilder())

    sender.start()

    while (true) {
      try {
        for (i <- 0 until 100) {
          val added = Int.box(Random.nextInt(500))
          val deleted = Int.box(Random.nextInt(100))
          val delta= Int.box((added - deleted))
          val obj = Map[String, AnyRef](
            "timestamp" -> new DateTime().toString,
            "page" -> "foo",
            "added" -> added,
            "deleted" -> deleted,
            "delta" -> delta
          )

          // Asynchronously send event to Druid:
          sender.send(obj.asJava) respond {
            case Return(_) =>
              log.info("Sent message.........")

            case Throw(e: MessageDroppedException) =>
              log.warn("Dropped message" + obj)

            case Throw(e) =>
              log.error("Failed to send message: %s", e)
          }
        }
      }
      finally {
        sender.flush()
        log.info("flushing...........................")
        //sender.stop()
      }
      Thread.sleep(60 * 1000)
    }
  }

}
