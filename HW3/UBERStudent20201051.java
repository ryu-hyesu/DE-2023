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

        JavaPairRDD<String, String> regionDayTripsVehicles = lines.mapToPair(
        	new PairFunction<String, String, String>(){ // input, outputK, outputV
			public Tuple2<String, String> call(String line){
				String[] parts = line.split(",");
				    String baseNumber = parts[0];
				    String dateString = parts[1];
				    int activeVehicles = Integer.parseInt(parts[2]);
				    int trips = Integer.parseInt(parts[3]);

				    LocalDate date = LocalDate.parse(dateString, DateTimeFormatter.ofPattern("M/d/yyyy"));
				    DayOfWeek dayOfWeek = date.getDayOfWeek();
				    String dayOfWeekString = dayOfWeek.getDisplayName(TextStyle.SHORT, Locale.US).toUpperCase();

				  

			  
				    String key = baseNumber + "," + dayOfWeekString;
				    String value = trips +","+ activeVehicles;

				    return new Tuple2(key, value);
			}
		
        });

        JavaPairRDD<String, String> regionDayTotalTripsVehicles = regionDayTripsVehicles.reduceByKey(new Function2<String, String, String>(){ 
        public String call(String v1, String v2){
            String[] parts1 = v1.split(",");
            String[] parts2 = v2.split(",");

            int totalTrips = Integer.parseInt(parts1[0]) + Integer.parseInt(parts2[0]);
            int totalVehicles = Integer.parseInt(parts1[1]) + Integer.parseInt(parts2[1]);

            return totalTrips  + "," + totalVehicles ;
        }});

        regionDayTotalTripsVehicles.saveAsTextFile(args[1]);

        spark.stop();
        
    }
}
