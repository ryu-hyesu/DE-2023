import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

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

        JavaPairRDD<String, Tuple2<Integer, Integer>> regionDayTripsVehicles = lines.mapToPair((PairFunction<String, String, Tuple2<Integer, Integer>>) line -> {
            String[] parts = line.split(",");
            String baseNumber = parts[0];
            String dateString = parts[1];
            int activeVehicles = Integer.parseInt(parts[2]);
            int trips = Integer.parseInt(parts[3]);

            String[] dateParts = dateString.split("/");
            String month = dateParts[0];
            String day = dateParts[1];
            String year = dateParts[2];

            String formattedDate = month + "/" + day + "/" + year.substring(2);

            LocalDate date = LocalDate.parse(formattedDate, DateTimeFormatter.ofPattern("M/d/yy"));
            DayOfWeek dayOfWeek = date.getDayOfWeek();
            String dayOfWeekString = dayOfWeek.getDisplayName(TextStyle.SHORT, Locale.US).toUpperCase();

            if (dayOfWeekString.equals("THU")) {
                dayOfWeekString = "THR";
            }

            String key = baseNumber + "," + dayOfWeekString;
            return new Tuple2<>(key, new Tuple2<>(activeVehicles, trips));
        });

        JavaPairRDD<String, Tuple2<Integer, Integer>> regionDayTotalTripsVehicles = regionDayTripsVehicles.reduceByKey((Function2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>>) (v1, v2) -> {
            int totalVehicles = v1._1() + v2._1();
            int totalTrips = v1._2() + v2._2();

            return new Tuple2<>(totalVehicles, totalTrips);
        });

        JavaPairRDD<String, String> regionDayTotalTripsVehiclesString = regionDayTotalTripsVehicles.mapToPair((PairFunction<Tuple2<String, Tuple2<Integer, Integer>>, String, String>) pair -> {
            String[] parts = pair._1().split(",");
            String baseNumber = parts[0];
            String dayOfWeekString = parts[1];
            String totalVehicles = String.valueOf(pair._2()._1());
            String totalTrips = String.valueOf(pair._2()._2());

            return new Tuple2<>(baseNumber + "," + dayOfWeekString, totalVehicles + "," + totalTrips);
        });

        regionDayTotalTripsVehiclesString.saveAsTextFile(args[1]);

        spark.stop();
    }
}
