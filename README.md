Spark To Kafka Writer
==============

APIs that help in writing data from Spark and Spark Streaming to Kafka. To use this in a Spark 
application, use Cloudera mvn repos: ``https://repository.cloudera.com/artifactory/cloudera-repos``
and ``https://repository.cloudera.com/artifactory/libs-snapshot-local`` for SNAPSHOTs.

Latest release artifact info, on maven:
```
  repository: https://repository.cloudera.com/artifactory/cloudera-repos
  groupId: org.cloudera.spark.streaming.kafka
  artifactId: spark-kafka-writer
  version: 0.1.0
```

To write to Kafka from a Spark app in Scala:

Import this object in this form:
```
  import org.cloudera.spark.streaming.kafka.KafkaWriter._
```

Once this is done, the `writeToKafka` method can be called on the `DStream` object in this form:

```
  dstream.writeToKafka(producerConfig, serializerFunc)
```

where the `producerConfig` is a properties object that defines the configuration that needs to be
passed to the Kafka producer, and `serializerFunc` is a function that takes the each element 
from the DStream and converts it into the format that has to be used to write to Kafka (the 
output of this method is passed as is to the Kafka producer).
 
To write an `RDD` to Kafka, there is an equivalent method, which can be used in the exact same 
way as above.
```
  rdd.writeToKafka(producerConfig, serializerFunc)
```

To write a Java DStream to Kafka:
```   
  JavaDStreamKafkaWriter<String> writer = JavaDStreamKafkaWriterFactory.fromJavaDStream(instream);
  writer.writeToKafka(producerConf, new ProcessingFunc());
```

Equivalently, a Java RDD can be written to Kafka in the following format:
```   
  JavaRDDKafkaWriter<String> writer = JavaRDDKafkaWriterFactory.fromJavaRDD(inrdd);
  writer.writeToKafka(producerConf, new ProcessingFunc());
```
`ProcessingFunc` must be an implementation of the `org.apache.spark.api.java.function.Function` 
class, and the `producerConf` is a `Properties` object containing the properties to be passed to 
Kafka Producer.

The current released version is 0.1.0, built against Apache Spark 1.3.0-cdh5.4.8 and  
Cloudera's distribution of Apache Kafka 0.8.2.0-kafka-1.3.2

[![Build Status](https://travis-ci.org/cloudera/spark-kafka-writer.svg?branch=master)](https://travis-ci.org/cloudera/spark-kafka-writer)
