package edu.njit.sparksql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;


import java.util.Scanner;

import static org.apache.spark.sql.functions.*;

public class MoreAggregations {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder().appName("SparkSql").master("local[*]")
                .getOrCreate();

        Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");

        spark.udf().register("hasPassed", (String grade, String subject) -> {
            return subject.equals("Biology") ? grade.startsWith("A") : (grade.startsWith("A") || grade.startsWith("B") || grade.startsWith("C"));
        }, DataTypes.BooleanType);

        /*dataset = dataset.groupBy(col("subject"), col("year"))
                .agg(max(col("score")).alias("max-score"), mean(col("score")).alias("mean-score"));*/
        dataset = dataset
                .groupBy(col("subject"))
                .pivot("year")
                .agg(round(avg(col("score")), 2).alias("avg"), round(stddev(col("score")), 2).alias("std-dev"));

        //dataset = dataset.withColumn("pass", callUDF("hasPassed", col("grade"), col("subject")));

        dataset.show(200);

        /*Scanner scanner = new Scanner(System.in);
        scanner.nextLine();*/
        spark.close();
    }
}
