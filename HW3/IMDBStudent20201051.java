import scala.Tuple2;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

public final class IMDBStudent20201051 implements Serializable {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: IMDBStudent20201051 <in-file> <out-file>");
            System.exit(1);
        }
        
        SparkSession spark = SparkSession
                .builder()
                .appName("IMDBStudent20201051")
                .getOrCreate();
        
        JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();

        JavaRDD<String> genres = lines.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String line) {
                String[] parts = line.split("::");
                String genreString = parts[2];
                String[] genreArray = genreString.split("\\|");
                return Arrays.asList(genreArray).iterator();
            }
        });

        JavaPairRDD<String, Integer> ones = genres.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String genre) {
                return new Tuple2<>(genre, 1);
            }
        });

        JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer x, Integer y) {
                return x + y;
            }
        });

        counts.saveAsTextFile(args[1]);
        spark.stop();
    }
}
