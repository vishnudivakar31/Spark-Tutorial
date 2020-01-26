package edu.njit;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.OneHotEncoderEstimator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.ml.tuning.TrainValidationSplit;
import org.apache.spark.ml.tuning.TrainValidationSplitModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class PropertyValuePredictor {

    public static void main(String[] args) {

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder()
                .appName("property-value=predcitor")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> csvData = spark
                                .read()
                                .option("header", true)
                                .option("inferSchema", true)
                                .csv("src/main/resources/kc_house_data.csv");

        csvData = csvData
                .withColumn("sqft_above_percentage", col("sqft_above").divide(col("sqft_living")))
                .withColumnRenamed("price", "label");

        Dataset<Row>[] dataSplit = csvData.randomSplit(new double[]{0.8, 0.2});
        Dataset<Row> trainAndTestData = dataSplit[0];
        Dataset<Row> holdOutData = dataSplit[1];

        StringIndexer conditionIndexer = new StringIndexer();
        conditionIndexer.setInputCol("condition");
        conditionIndexer.setOutputCol("condition_index");

        StringIndexer gradeIndexer = new StringIndexer();
        gradeIndexer.setInputCol("grade");
        gradeIndexer.setOutputCol("grade_index");

        StringIndexer zipcodeIndexer = new StringIndexer();
        zipcodeIndexer.setInputCol("zipcode");
        zipcodeIndexer.setOutputCol("zipcode_index");

        OneHotEncoderEstimator estimator = new OneHotEncoderEstimator();
        estimator.setInputCols(new String[] {"condition_index", "grade_index", "zipcode_index"});
        estimator.setOutputCols(new String[] {"condition_vector", "grade_vector", "zipcode_vector"});


        VectorAssembler vectorAssembler = new VectorAssembler()
                .setInputCols(new String[]{"bedrooms", "bathrooms", "sqft_living", "sqft_above_percentage",
                        "floors", "condition_vector", "grade_vector", "zipcode_vector", "waterfront"})
                .setOutputCol("features");

        LinearRegression linearRegression = new LinearRegression();
        ParamGridBuilder paramGridBuilder = new ParamGridBuilder();

        ParamMap[] paramMaps = paramGridBuilder
                .addGrid(linearRegression.regParam(), new double[] {0.01, 0.1, 0.5})
                .addGrid(linearRegression.elasticNetParam(), new double[] {0, 0.5, 1})
                .build();

        TrainValidationSplit trainValidationSplit = new TrainValidationSplit()
                .setEstimator(linearRegression)
                .setEvaluator(new RegressionEvaluator().setMetricName("r2"))
                .setEstimatorParamMaps(paramMaps)
                .setTrainRatio(0.8);

        Pipeline pipeline = new Pipeline();
        pipeline.setStages(new PipelineStage[] {
                conditionIndexer,
                gradeIndexer,
                zipcodeIndexer,
                estimator,
                vectorAssembler,
                trainValidationSplit
        });

        PipelineModel pipelineModel = pipeline.fit(trainAndTestData);
        TrainValidationSplitModel validationModel = (TrainValidationSplitModel) pipelineModel.stages()[5];

        Dataset<Row> holdOutDataResults = pipelineModel.transform(holdOutData);
        holdOutDataResults = holdOutDataResults.drop("prediction");

        LinearRegressionModel model = (LinearRegressionModel) validationModel.bestModel();

        System.out.println("Traing data's R^2 is " + model.summary().r2());
        System.out.println("Traing data's RMSE is " + model.summary().rootMeanSquaredError());

        model.transform(holdOutDataResults).show();

        System.out.println("Testing data's R^2 is " + model.evaluate(holdOutDataResults).r2());
        System.out.println("Testing data's RMSE is " + model.evaluate(holdOutDataResults).rootMeanSquaredError());

        spark.close();

    }
}
