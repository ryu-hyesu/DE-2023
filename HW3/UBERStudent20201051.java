import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Iterator;
import java.util.Locale;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.TextStyle;

public final class UBERStudent20201051 {
    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: UBERStudent20201051 <in-file> <out-file>");
            System.exit(1);
        }

        SparkSession spark = SparkSession.builder()
                .appName("UBERStudent20201051")
                .getOrCreate();

        JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();

        JavaPairRDD<String, String> regionDayTripsVehicles = lines.mapToPair((PairFunction<String, String, String>) line -> {
            String[] parts = line.split(",");
            String baseNumber = parts[0];
            String dateString = parts[1];
            int activeVehicles = Integer.parseInt(parts[2]);
            int trips = Integer.parseInt(parts[3]);

            LocalDate date = LocalDate.parse(dateString, DateTimeFormatter.ofPattern("M/d/yyyy"));
            DayOfWeek dayOfWeek = date.getDayOfWeek();
            String dayOfWeekString = dayOfWeek.getDisplayName(TextStyle.SHORT, Locale.US).toUpperCase();

            if (dayOfWeekString.equals("THU")) {
                dayOfWeekString = "THR";
            }

            String key = baseNumber + "," + dayOfWeekString;
            String value = activeVehicles + "," + trips;

            return new Tuple2<>(key, value);
        });

        JavaPairRDD<String, Tuple2<Integer, Integer>> regionDayTotalTripsVehicles = regionDayTripsVehicles.reduceByKey((Function2<String, String, Tuple2<Integer, Integer>>) (v1, v2) -> {
            String[] parts1 = v1.split(",");
            String[] parts2 = v2.split(",");

            int totalTrips = Integer.parseInt(parts1[1]) + Integer.parseInt(parts2[1]);
            int totalVehicles = Integer.parseInt(parts1[0]) + Integer.parseInt(parts2[0]);

            return new Tuple2<>(totalVehicles, totalTrips);
        });

        regionDayTotalTripsVehicles.saveAsTextFile(args[1]);

        spark.stop();
    }
}
