/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.holdenkarau.spark.testing.kafka

import java.util.Collections

import net.manub.embeddedkafka.EmbeddedKafka
import org.apache.kafka.common.serialization.{Deserializer, Serializer, StringDeserializer, StringSerializer}
import org.junit.runner.RunWith
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.concurrent.Waiters.timeout
import org.scalatest.junit.JUnitRunner
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{Assertion, BeforeAndAfterAll, FunSuite}

@RunWith(classOf[JUnitRunner])
class KafkaTestUtilsTest extends FunSuite with BeforeAndAfterAll {
  implicit val serializer: Serializer[String] = new StringSerializer()
  implicit val deserializer: Deserializer[String] = new StringDeserializer()

  override def beforeAll(): Unit = {
    EmbeddedKafka.start()
  }

  override def afterAll(): Unit = {
    EmbeddedKafka.stop()
  }

  test("Kafka send and receive message") {
    val topic = "test-topic"
    val message = "HelloWorld!"
    EmbeddedKafka.createCustomTopic(topic)
    EmbeddedKafka.publishToKafka(topic, message)

    EmbeddedKafka.withConsumer[String, String, Assertion](consumer => {
      consumer.subscribe(Collections.singletonList(topic))

      eventually (timeout(Span(5, Seconds))) {
        import scala.collection.JavaConverters._
        val records = consumer.poll(java.time.Duration.ofMillis(1000)).asScala
        assert(records.head.value() === message)
      }
    })
  }
}
