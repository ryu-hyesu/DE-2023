import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

public final class IMDBStudent20201051 {
    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: IMDBStudent20201051 <in-file> <out-file>");
            System.exit(1);
        }

        SparkSession spark = SparkSession.builder()
                .appName("IMDBStudent20201051")
                .getOrCreate();

        JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();

        JavaRDD<String> words = lines.flatMap((FlatMapFunction<String, String>) s -> {
            String title = s.split("::")[2];
            return Arrays.asList(title.split("|")).iterator();
        });

        JavaPairRDD<String, Integer> ones = words.mapToPair((PairFunction<String, String, Integer>) word -> new Tuple2<>(word, 1));

        JavaPairRDD<String, Integer> counts = ones.reduceByKey((Function2<Integer, Integer, Integer>) (x, y) -> x + y);

        counts.saveAsTextFile(args[1]);

        spark.stop();
    }
}
