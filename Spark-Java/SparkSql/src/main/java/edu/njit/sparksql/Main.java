package edu.njit.sparksql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.functions.*;

import java.text.SimpleDateFormat;
import java.util.*;

public class Main {
    public static void main(String[] args) {

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder().appName("SparkSql").master("local[*]")
                .getOrCreate();

        Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");

        //Dataset<Row> modernArtResults = dataset.filter("subject = 'Modern Art' AND year >= 2007");

       /* Dataset<Row> modernArtResults = dataset
                .filter(row -> row.getAs("subject").equals("Modern Art") && Integer.parseInt(row.getAs("year")) >= 2007);*/

        /*Column subjectColumn = dataset.col("subject");
        Column yearColumn = dataset.col("year");

        Dataset<Row> modernArtResults = dataset.filter(subjectColumn.equalTo("Modern Art")
                .and(yearColumn.geq(2007)));*/

        /*dataset.createOrReplaceTempView("my_students_table");

        Dataset<Row> frenchResults = spark.sql("select distinct(year) from my_students_table order by year desc");

        frenchResults.show();*/

        /*List<Row> inMemory = new ArrayList<>();
        inMemory.add(RowFactory.create("WARN", "2016-12-31 04:19:32"));
        inMemory.add(RowFactory.create("FATAL", "2016-12-31 03:22:34"));
        inMemory.add(RowFactory.create("WARN", "2016-12-31 03:21:21"));
        inMemory.add(RowFactory.create("INFO", "2015-4-21 14:32:21"));
        inMemory.add(RowFactory.create("FATAL","2015-4-21 19:23:20"));

        StructField[] fields = new StructField[] {
          new StructField("level", DataTypes.StringType, false, Metadata.empty()),
                new StructField("datetime", DataTypes.StringType, false, Metadata.empty())
        };

        StructType schema = new StructType(fields);
        Dataset<Row> inMemoryDataset = spark.createDataFrame(inMemory, schema);*/

        // inMemoryDataset.show();
        SimpleDateFormat sdf = new SimpleDateFormat("MMMM");
        SimpleDateFormat monthNum = new SimpleDateFormat("M");
        Dataset<Row> logData = spark.read().option("header", true).csv("src/main/resources/biglog.txt");
        /*logData.createOrReplaceTempView("logging_table");

        spark.udf().register("monthNum", (String rawMonth) -> {
            Date inputDate = sdf.parse(rawMonth);
            return Integer.parseInt(monthNum.format(inputDate));
        }, DataTypes.IntegerType);
*/
        /*logData.createOrReplaceTempView("logging_table");



        results.createOrReplaceTempView("total_results");

        Dataset<Row> totalResult = spark.sql("select sum(total) from total_results");
        totalResult.show();*/

       /* logData = logData.select(col("level"), date_format(col("datetime"), "MMMM").alias("month"),
                date_format(col("datetime"), "M").alias("monthNum").cast(DataTypes.IntegerType));
*/
        /*logData = logData.groupBy(col("level"), col("month"), col("monthNum")).count();
        logData = logData.orderBy("monthNum");
        logData = logData.drop(col("monthNum"));*/


        /*Object[] months = new Object[] {"January", "February", "March", "April", "May", "June", "July", "August", "September", "November", "December"};
        List<Object> columns = Arrays.asList(months);

        logData = logData.groupBy(col("level")).pivot("month", columns).count();*/

        Dataset<Row> results = logData.select(col("level"),
                date_format(col("datetime"), "MMMM").alias("month"),
                date_format(col("datetime"), "M").alias("monthNum").cast(DataTypes.IntegerType));
        results = results.groupBy("level", "month", "monthNum").count().as("total").orderBy(col("monthNum"));
        results = results.drop("monthNum");
        results.show(100);

        Scanner scanner = new Scanner(System.in);
        scanner.nextLine();

        spark.close();

    }
}
