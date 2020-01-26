package edu.njit.sparkhelloworld;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.sql.sources.In;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Main {
    public static void main(String[] args) {

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        List<Tuple2<Integer, Integer>> visitsRaw = new ArrayList<>();
        List<Tuple2<Integer, String>> userRaw = new ArrayList<>();
        SparkConf sparkConf = new SparkConf().setAppName("SparkHelloWorld").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        visitsRaw.add(new Tuple2<>(4, 18));
        visitsRaw.add(new Tuple2<>(6, 4));
        visitsRaw.add(new Tuple2<>(10, 9));

        userRaw.add(new Tuple2<>(1, "John"));
        userRaw.add(new Tuple2<>(2, "Bob"));
        userRaw.add(new Tuple2<>(3, "Alan"));
        userRaw.add(new Tuple2<>(4, "Doris"));
        userRaw.add(new Tuple2<>(5, "Mary"));
        userRaw.add(new Tuple2<>(6, "Raquel"));

        JavaPairRDD<Integer, Integer> visits = sc.parallelizePairs(visitsRaw);
        JavaPairRDD<Integer, String> users = sc.parallelizePairs(userRaw);

        JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Integer, String>> joinedData = visits.cartesian(users);

        joinedData.collect().forEach(value -> System.out.println(value));

        sc.close();
    }
}
