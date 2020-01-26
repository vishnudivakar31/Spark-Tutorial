package edu.njit;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Top Viewed Course Using Spark
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        Boolean testMode = false; // Set false in production

        SparkConf conf = new SparkConf().setAppName("TopViewedCourses").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaPairRDD<Integer, Integer> chapterData = getChapterDataRdd(sc, testMode);
        JavaPairRDD<Integer, String> titleData = getTitleDataRdd(sc, testMode);
        JavaPairRDD<Integer, Integer> viewData = getViewDataRdd(sc, testMode);

        JavaPairRDD<Integer, Integer> chapterCountRdd = chapterData.mapToPair(row -> new Tuple2<>(row._2, 1))
                .reduceByKey((value1, value2) -> value1 + value2);


        // Remove duplicate view
        viewData = viewData.distinct();

        // Flip View data to make chapter id as key and join with chapter data
        viewData = viewData.mapToPair(view -> new Tuple2<>(view._2, view._1));
        JavaPairRDD<Integer, Tuple2<Integer, Integer>> joinedViewAndChapter = viewData.join(chapterData);

        // Remove chapter id and reduce to find how many chapters each user has watched
        JavaPairRDD<Tuple2<Integer, Integer>, Long> viewCountByUser = joinedViewAndChapter.mapToPair(view -> new Tuple2<>(view._2, 1L))
                .reduceByKey((value1, value2) -> value1 + value2);

        // Remove user id and simplyfy viewCountByUser
        JavaPairRDD<Integer, Long> viewCount = viewCountByUser.mapToPair(view -> new Tuple2<>(view._1._2, view._2));

        // Add in total chapter count
        JavaPairRDD<Integer, Tuple2<Long, Integer>> viewCountWithChapterCount = viewCount.join(chapterCountRdd);

        // Find View Ratio
        JavaPairRDD<Integer, Double> viewRatioPerChapter = viewCountWithChapterCount.mapValues(value -> (double) value._1 / value._2);

        // Convert ratio to score
        JavaPairRDD<Integer, Long> scoresByChapter = viewRatioPerChapter.mapValues(ratio -> {
            if (ratio > 0.9) return 10L;
            else if (ratio > 0.5) return 4L;
            else if (ratio > 0.25) return 2L;
            else return 0L;
        });

        // Reduce by key to find total score
        scoresByChapter = scoresByChapter.reduceByKey((value1, value2) -> value1 + value2);
        
        // Combine scores with chapter title
        JavaPairRDD<Integer, Tuple2<Long, String>> resultWithTitle = scoresByChapter.join(titleData);

        // Remove Chapter id and sort by score
        JavaPairRDD<Long, String> result = resultWithTitle.mapToPair(value -> new Tuple2<>(value._2._1, value._2._2));
        result.sortByKey(false).collect().forEach(System.out::println);

        sc.close();
    }

    private static JavaPairRDD<Integer, Integer> getViewDataRdd(JavaSparkContext sc, Boolean testMode) {
        if(testMode) {
            List<Tuple2<Integer, Integer>> viewData = new ArrayList<>();
            viewData.add(new Tuple2<>(14, 96));
            viewData.add(new Tuple2<>(14, 97));
            viewData.add(new Tuple2<>(13, 96));
            viewData.add(new Tuple2<>(13, 96));
            viewData.add(new Tuple2<>(13, 96));
            viewData.add(new Tuple2<>(14, 99));
            viewData.add(new Tuple2<>(13, 100));
            return sc.parallelizePairs(viewData);
        }
        return sc.textFile("src/main/resources/viewing figures/views-*.csv")
                .mapToPair(commaSeparatedValue -> {
                    String[] columns = commaSeparatedValue.split(",");
                    Integer userId = Integer.parseInt(columns[0]);
                    Integer chapterId = Integer.parseInt(columns[1]);
                    return new Tuple2<>(userId, chapterId);
                });
    }

    private static JavaPairRDD<Integer, String> getTitleDataRdd(JavaSparkContext sc, Boolean testMode) {
        if(testMode) {
            List<Tuple2<Integer, String>> titleData = new ArrayList<>();
            titleData.add(new Tuple2<>(1, "How to find a better job"));
            titleData.add(new Tuple2<>(2, "Work faster harder smarter until you drop"));
            titleData.add(new Tuple2<>(3, "Content Creation is a Mug's Game"));
            return sc.parallelizePairs(titleData);
        }
        return sc.textFile("src/main/resources/viewing figures/titles.csv")
                .mapToPair(commaSeparatedValue -> {
                    String[] columns = commaSeparatedValue.split(",");
                    Integer courseId = Integer.parseInt(columns[0]);
                    String title = columns[1];
                    return new Tuple2<>(courseId, title);
                });
    }

    private static JavaPairRDD<Integer, Integer> getChapterDataRdd(JavaSparkContext sc, Boolean testMode) {
        if(testMode) {
            List<Tuple2<Integer, Integer>> chapterData = new ArrayList<>();
            chapterData.add(new Tuple2<>(96,  1));
            chapterData.add(new Tuple2<>(97,  1));
            chapterData.add(new Tuple2<>(98,  1));
            chapterData.add(new Tuple2<>(99,  2));
            chapterData.add(new Tuple2<>(100, 3));
            chapterData.add(new Tuple2<>(101, 3));
            chapterData.add(new Tuple2<>(102, 3));
            chapterData.add(new Tuple2<>(103, 3));
            chapterData.add(new Tuple2<>(104, 3));
            chapterData.add(new Tuple2<>(105, 3));
            chapterData.add(new Tuple2<>(106, 3));
            chapterData.add(new Tuple2<>(107, 3));
            chapterData.add(new Tuple2<>(108, 3));
            chapterData.add(new Tuple2<>(109, 3));
            return sc.parallelizePairs(chapterData);
        }
        return sc.textFile("src/main/resources/viewing figures/chapters.csv")
                .mapToPair(commaSeparatedValue -> {
                    String[] columns = commaSeparatedValue.split(",");
                    Integer chapterId = Integer.parseInt(columns[0]);
                    Integer courseId = Integer.parseInt(columns[1]);
                    return new Tuple2<>(chapterId, courseId);
                });
    }
}
