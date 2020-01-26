package edu.njit.stream;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

public class App {
    public static void main(String[] args) throws InterruptedException {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);

        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("StreamingApp");
        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(2));

        JavaReceiverInputDStream<String> inputData = sc.socketTextStream("localhost", 8989);
        JavaPairDStream<String, Long> pairDStream = inputData.mapToPair(item -> new Tuple2<>(item.split(",")[0], 1L));
        JavaPairDStream<String, Long> results = pairDStream.reduceByKeyAndWindow((value1, value2) -> value1 + value2, Durations.minutes(2));

        results.print();

        sc.start();
        sc.awaitTermination();
    }
}
