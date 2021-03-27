import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;

public class G14HW1 {
    public static void main(String[] args) throws IOException {

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // CHECKING NUMBER OF CMD LINE PARAMETERS
        // Parameters are: num_partitions, prodLargestNRating, <path_to_file>
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        if (args.length != 3) {
            throw new IllegalArgumentException("USAGE: num_partitions prodLargestNRating file_path");
        }

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // SPARK SETUP
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        SparkConf conf = new SparkConf(true).setAppName("HW1");   // Give a name to the application
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");     // How many messages of log comes out

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // INPUT READING
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        // Read number of partitions
        int K = Integer.parseInt(args[0]);
        // Read number of partitions
        int T = Integer.parseInt(args[1]);

        // Read input file and subdivide it into K random partitions
        JavaRDD<String> RawData = sc.textFile(args[2]).repartition(K).cache();

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // SETTING GLOBAL VARIABLES
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&


        System.out.println("Dataset dimension = " + RawData.count());
        JavaPairRDD<String, Float> count;    // RDD of KV pairs
        Random randomGenerator = new Random();


        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // IMPROVED WORD COUNT (keys in [0,K-1]) with groupByKey
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        count = RawData
                .flatMapToPair((document) -> {    // <-- MAP PHASE (R1)
                    String[] tokens = document.split(",");

                    ArrayList<Tuple2<String, Tuple2<String, Float>>> pairs = new ArrayList<>();
                    pairs.add(new Tuple2<>(tokens[1], new Tuple2<>(tokens[0], Float.parseFloat(tokens[2]))));

                    return pairs.iterator();
                })
                .groupByKey()    // <-- REDUCE PHASE (R1)
                .flatMapToPair((element) -> {
                    float avg = 0;
                    int n = 0;
                    for (Tuple2<String, Float> c : element._2()) {
                        avg += c._2();
                        n++;
                    }

                    ArrayList<Tuple2<String, Float>> normalizedRatings = new ArrayList<>();
                    for (Tuple2<String, Float> c : element._2()) {
                        normalizedRatings.add(new Tuple2<>(c._1(), c._2() - avg/n));
                    }
                    return normalizedRatings.iterator();
                })
                .reduceByKey((x, y) -> Math.max(x,y)); // <-- REDUCE PHASE (R2)

        count.mapToPair(pair -> new Tuple2<Float , String>(pair._2, pair._1)).sortByKey(false).take(T)
                .forEach(pair -> System.out.println(pair._2 + " -> " + pair._1));
        System.out.println("Dataset dimension = " + count.count());


    }

}
