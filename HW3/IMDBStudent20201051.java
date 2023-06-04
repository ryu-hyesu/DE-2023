import scala.Tuple2;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

public final class IMDBGenreCount implements Serializable {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: IMDBGenreCount <in-file> <out-file>");
            System.exit(1);
        }

        SparkSession spark = SparkSession
                .builder()
                .appName("IMDBGenreCount")
                .getOrCreate();

        JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();

        JavaPairRDD<String, Integer> counts = lines.flatMapToPair(new PairFlatMapFunction<String, String, Integer>() {
            @Override
            public Iterator<Tuple2<String, Integer>> call(String line) throws Exception {
                String[] parts = line.split("::");
                String[] genres = parts[2].split("\\|");

                List<Tuple2<String, Integer>> genreCounts = new ArrayList<>();

                for (String genre : genres) {
                    genreCounts.add(new Tuple2<>(genre, 1));
                }

                return genreCounts.iterator();
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer count1, Integer count2) throws Exception {
                return count1 + count2;
            }
        });

        counts.saveAsTextFile(args[1]);
        spark.stop();
    }
}
