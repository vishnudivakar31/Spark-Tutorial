package edu.njit.kmeansclustering;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

import static org.apache.spark.sql.functions.*;

public class RecommendSystem {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder().appName("RecommendSystem").master("local[*]").getOrCreate();

        Dataset<Row> csvData = spark.read()
                .option("inferSchema", true)
                .option("header", true)
                .csv("src/main/resources/VPPcourseViews.csv");

        csvData = csvData.withColumn("proportionWatched", col("proportionWatched").multiply(100));

        //csvData = csvData.groupBy("userId").pivot("courseId").sum("proportionWatched");


        ALS als = new ALS()
                .setMaxIter(10)
                .setRegParam(0.1)
                .setUserCol("userId")
                .setItemCol("courseId")
                .setRatingCol("proportionWatched");

        ALSModel model = als.fit(csvData);

        Dataset<Row> userRecs = model.recommendForAllUsers(5);

        List<Row> userRecsList = userRecs.takeAsList(5);

        for(Row r : userRecsList) {
            int userId = r.getAs(0);
            String recs = r.getAs(1).toString();
            System.out.println("UserId: " + userId + ", recs: " + recs);
            System.out.println("This user already watched");
            csvData.filter("userId = " + userId).show();
        }

        spark.close();
    }
}
