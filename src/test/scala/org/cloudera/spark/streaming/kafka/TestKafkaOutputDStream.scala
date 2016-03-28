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
package org.cloudera.spark.streaming.kafka

import java.util.Properties

import org.apache.kafka.clients.producer.ProducerRecord
import org.cloudera.spark.streaming.kafka.util.TestUtil
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.junit.{After, Before, Test, Assert}
import org.cloudera.spark.streaming.kafka.KafkaWriter._

import scala.collection.mutable

class TestKafkaOutputDStream {
  private val testUtil: TestUtil = TestUtil.getInstance
  // Name of the framework for Spark context
  def framework = this.getClass.getSimpleName

  // Master for Spark context
  def master = "local[2]"
  val conf = new SparkConf()
    .setMaster(master)
    .setAppName(framework)
//  conf.set("spark.streaming.clock", "org.apache.spark.streaming.util.ManualClock")
  val ssc = new StreamingContext(conf, Duration.apply(2000))

  @Before
  def setup() {
    testUtil.prepare()
    val topics: java.util.List[String] = new java.util.ArrayList[String](3)
    topics.add("default")
    topics.add("static")
    topics.add("custom")
    testUtil.initTopicList(topics)
  }

  @After
  def tearDown(): Unit = {
    testUtil.tearDown()
  }

  @Test
  def testKafkaDStream(): Unit = {
    val toBe = new mutable.Queue[RDD[String]]()
    var j = 0
    while (j < 9) {
      toBe.enqueue(ssc.sparkContext.makeRDD(Seq(j.toString, (j + 1).toString, (j + 2).toString)))
      j += 3
    }
    val instream = ssc.queueStream(toBe)
    val producerConf = new Properties()
    producerConf.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    producerConf.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
    producerConf.put("bootstrap.servers", testUtil.getKafkaServerUrl)
    producerConf.put("request.required.acks", "1")
    instream.writeToKafka(producerConf,
      (x: String) => new ProducerRecord[String,Array[Byte]]("default", null,x.getBytes))
      ssc.start()

    Thread.sleep(10000)
    val expectedResults = (0 to 8).map(_.toString).toSeq
    val actualResults = new mutable.HashSet[String]()
    var moreMessages = true
    while (moreMessages) {
      val rawMsg = testUtil.getNextMessageFromConsumer("default")
      if (rawMsg != null) {
        val fetchedMsg = new String(rawMsg.message.asInstanceOf[Array[Byte]])
        Assert.assertNotNull(fetchedMsg)
        actualResults += fetchedMsg
      } else {
        moreMessages = false
      }
    }
    val actualResultSorted = actualResults.toSeq.sorted
    Assert.assertEquals(expectedResults, actualResultSorted)
  }
}
