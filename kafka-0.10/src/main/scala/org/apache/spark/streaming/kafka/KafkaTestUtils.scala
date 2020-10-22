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
package org.apache.spark.streaming.kafka

import net.manub.embeddedkafka.EmbeddedKafka
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer, Deserializer, Serializer}
import org.apache.spark.Logging

/**
 * This is a helper class for Kafka test suites. This has the functionality to set up
 * and tear down local Kafka servers, and to push data using Kafka producers.
 *
 */
class KafkaTestUtils extends Logging with EmbeddedKafka{
  implicit val serializer: Serializer[Array[Byte]] = new ByteArraySerializer()
  implicit val deserializer: Deserializer[Array[Byte]] = new ByteArrayDeserializer()

  /** setup the whole embedded servers, including Zookeeper and Kafka brokers */
  def setup(): Unit = {
    EmbeddedKafka.start()
  }

  def teardown(): Unit = {
    EmbeddedKafka.stop()
  }

  /** Create a Kafka topic and wait until it propagated to the whole cluster */
  def createTopic(topic: String): Unit = {
    createCustomTopic(topic)
  }

  /** Send the array of messages to the Kafka broker */
  def sendMessages(topic: String, message: Array[Byte]): Unit = {
    publishToKafka(topic, message)
  }
}
