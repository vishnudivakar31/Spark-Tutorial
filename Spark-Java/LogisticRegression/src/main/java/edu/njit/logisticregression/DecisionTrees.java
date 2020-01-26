package edu.njit.logisticregression;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.classification.DecisionTreeClassificationModel;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.classification.RandomForestClassificationModel;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.IndexToString;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.DecisionTreeRegressionModel;
import org.apache.spark.ml.regression.DecisionTreeRegressor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class DecisionTrees {
    public static UDF1<String,String> countryGrouping = new UDF1<String,String>() {

        public String call(String country) throws Exception {
            List<String> topCountries =  Arrays.asList(new String[] {"GB","US","IN","UNKNOWN"});
            List<String> europeanCountries =  Arrays.asList(new String[] {"BE","BG","CZ","DK","DE","EE","IE","EL","ES",
                    "FR","HR","IT","CY","LV","LT","LU","HU","MT","NL","AT","PL","PT","RO","SI","SK","FI",
                    "SE","CH","IS","NO","LI","EU"});

            if (topCountries.contains(country)) return country;
            if (europeanCountries .contains(country)) return "EUROPE";
            else return "OTHER";
        }

    };
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder().appName("DecisionTrees").master("local[*]").getOrCreate();

        Dataset<Row> csvData = spark.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("src/main/resources/vppFreeTrials.csv");
        spark.udf().register("countryGrouping", countryGrouping, DataTypes.StringType);

        csvData = csvData
                .withColumn("country", callUDF("countryGrouping", col("country")))
                .withColumn("label", when(col("payments_made").geq(1), lit(1)).otherwise(lit(0)));

        StringIndexer countryIndexer = new StringIndexer();
        csvData = countryIndexer
                .setInputCol("country")
                .setOutputCol("countryIndex")
                .fit(csvData)
                .transform(csvData);

        Dataset<Row> countryData = csvData.select(col("countryIndex")).distinct();
        countryData = new IndexToString().setInputCol("countryIndex").setOutputCol("value").transform(countryData);
        countryData.show();

        VectorAssembler vectorAssembler = new VectorAssembler();
        vectorAssembler.setInputCols(new String[] {"countryIndex", "rebill_period", "chapter_access_count", "seconds_watched"});
        vectorAssembler.setOutputCol("features");

        Dataset<Row> inputData = vectorAssembler.transform(csvData).select("label", "features");

        Dataset<Row>[] trainingAndHoldoutData = inputData.randomSplit(new double[] {0.8, 0.2});
        Dataset<Row> trainingData = trainingAndHoldoutData[0], holdOutData = trainingAndHoldoutData[1];

        DecisionTreeClassifier decisionTreeClassifier = new DecisionTreeClassifier();
        decisionTreeClassifier.setMaxDepth(3);

        DecisionTreeClassificationModel model = decisionTreeClassifier.fit(trainingData);

        Dataset<Row> predictions = model.transform(holdOutData);
        predictions.show();

        MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator();
        evaluator.setMetricName("accuracy");

        System.out.println("Accuracy is " + evaluator.evaluate(predictions));

        RandomForestClassifier rfClassifier = new RandomForestClassifier();
        rfClassifier.setMaxDepth(6);
        RandomForestClassificationModel rfModel = rfClassifier.fit(trainingData);

        Dataset<Row> predictions2 = rfModel.transform(holdOutData);
        predictions2.show();

        System.out.println("Accuracy is " + evaluator.evaluate(predictions2));

        spark.close();
    }
}
