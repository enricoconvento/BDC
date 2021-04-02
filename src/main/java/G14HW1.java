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
        // Read desired number of  products with largest maximum normalized rating
        int T = Integer.parseInt(args[1]);

        // Read input file and subdivide it into K random partitions
        JavaRDD<String> RawData = sc.textFile(args[2]).repartition(K).cache();

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // SETTING GLOBAL VARIABLES
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&


        System.out.println("Dataset starting dimension = " + RawData.count());
        System.out.println("Number of partitons K = " + K);
        System.out.println("Number of products with largest maximum normalized rating  T = " + T);
        JavaPairRDD<String, Float> maxNormRatings ;    // RDD of KV pairs
        Random randomGenerator = new Random();


        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        //
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        maxNormRatings = RawData                                                                // cambiato count in maxNormRatings
                .flatMapToPair((review ) -> {    // <-- MAP PHASE (R1)                                           cambiato document con review
                    String[] tokens = review .split(",");

                    ArrayList<Tuple2<String, Tuple2<String, Float>>> pairs = new ArrayList<>();
                    pairs.add(new Tuple2<>(tokens[1], new Tuple2<>(tokens[0], Float.parseFloat(tokens[2]))));
                    // MAP into  (    UserID            ,(      ProductID ,         Rating       ))
                    return pairs.iterator();
                })
                .groupByKey()    // <-- REDUCE PHASE (R1)
                .flatMapToPair((element) -> {
                    // Compute average of rating AvgRating
                    float AvgRating = 0;
                    int n = 0;
                    for (Tuple2<String, Float> c : element._2()) {
                        AvgRating += c._2();
                        n++;
                    }
                    // Apply normalization NormRating=Rating-AvgRating
                    // For each UserID : (UserID, ( ProductID, Rating)) ->  (ProductID,NormRating)
                    ArrayList<Tuple2<String, Float>> normalizedRatings = new ArrayList<>();
                    for (Tuple2<String, Float> c : element._2()) {
                        normalizedRatings.add(new Tuple2<>(c._1(), c._2() - AvgRating/n));
                    }
                    return normalizedRatings.iterator();
                })
                .reduceByKey((x, y) -> Math.max(x,y)); // <-- REDUCE PHASE (R2)
                // Compute the maximum of the ratings, reduceByKey(f) method can be used because the operator
                // max(*,*) is cumulative and associative

        // Extract the T maximum rated products
        // At this point we got pairs of the form (ProductID,maxRating), swap key and value, in this way we can
        // sort by key an then we take out the first T entries

        maxNormRatings.mapToPair(pair -> new Tuple2<>(pair._2, pair._1)).sortByKey(false).take(T)
                .forEach(pair -> System.out.println("Rating of "+ pair._2 + " -> " + pair._1));


        System.out.println("Dataset final dimension = " + maxNormRatings .count());


    }

}