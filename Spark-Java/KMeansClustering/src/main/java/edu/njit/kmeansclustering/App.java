package edu.njit.kmeansclustering;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.evaluation.ClusteringEvaluator;
import org.apache.spark.ml.feature.OneHotEncoderEstimator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

public class App {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder().appName("K-Means Clustering").master("local[*]").getOrCreate();
        Dataset<Row> csvData = spark.read()
                .option("inferSchema", true)
                .option("header", true)
                .csv("src/main/resources/GymCompetition.csv");

        csvData = new StringIndexer()
                .setInputCol("Gender")
                .setOutputCol("GenderIndex")
                .fit(csvData)
                .transform(csvData);

        csvData = new OneHotEncoderEstimator()
                .setInputCols(new String[] {"GenderIndex"})
                .setOutputCols(new String[] {"GenderVector"})
                .fit(csvData)
                .transform(csvData);

        VectorAssembler vectorAssembler = new VectorAssembler();
        Dataset<Row> inputData = vectorAssembler
                .setInputCols(new String[]{"GenderVector", "Age", "Height", "Weight", "NoOfReps"})
                .setOutputCol("features")
                .transform(csvData)
                .select("features");

        KMeans kMeans = new KMeans();
        kMeans.setK(5);

        KMeansModel model = kMeans.fit(inputData);

        Dataset<Row> predictions = model.transform(inputData);

      /*  Vector[] clusterCenters = model.clusterCenters();

        for(Vector v : clusterCenters) {
            System.out.println(v);
        }*/

        predictions.show();

        predictions.groupBy("prediction").count().show();

        System.out.println("SSE is " + model.computeCost(inputData));

        ClusteringEvaluator evaluator = new ClusteringEvaluator();

        System.out.println("Euclidean distance is " + evaluator.evaluate(predictions));

        spark.close();
    }
}
