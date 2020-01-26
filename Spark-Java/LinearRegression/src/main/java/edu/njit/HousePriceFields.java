package edu.njit;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class HousePriceFields {
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

        csvData = csvData.drop("id", "date", "waterfront", "view", "condition", "grade",
                "yr_renovated", "zipcode", "lat", "long", "sqft_lot", "sqft_lot15", "yr_built", "sqft_living15");

        for(String column : csvData.columns()) {
            System.out.println("Correlation between price and " + column + " is " + csvData.stat().corr("price", column));
        }

        spark.close();

    }
}
