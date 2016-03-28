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
import org.apache.spark.api.java.function.Function

import scala.reflect.ClassTag

import org.apache.spark.api.java.JavaRDD

class JavaRDDKafkaWriter[T](rdd: JavaRDD[T])(implicit val classTag: ClassTag[T]) {
  private val rddWriter = new RDDKafkaWriter[T](rdd)

  def writeToKafka[K, V](
      producerConfig: Properties,
      function1: Function[T, ProducerRecord[K,V]]): Unit = {
    rddWriter.writeToKafka(producerConfig, t => function1.call(t))
  }

  def writeListToKafka[K, V](
      producerConfig: Properties,
      function1: Function[T, java.util.List[ProducerRecord[K,V]]]): Unit = {
    rddWriter.writeListToKafka(producerConfig, t => function1.call(t))
  }

}

object JavaRDDKafkaWriterFactory {

  def fromJavaRDD[T](rdd: JavaRDD[T]): JavaRDDKafkaWriter[T] = {
    implicit val cmt: ClassTag[T] =
      implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[T]]
    new JavaRDDKafkaWriter[T](rdd)
  }
}
