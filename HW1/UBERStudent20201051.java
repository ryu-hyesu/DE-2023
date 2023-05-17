import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class UBERStudent20201051 {
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        private Text regionDay = new Text();
        private Text tripsVehicles = new Text();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line, ",");

            // Extracting the values from the input
            String baseNumber = tokenizer.nextToken();
            String dayOfWeek = tokenizer.nextToken();
            int trips = Integer.parseInt(tokenizer.nextToken());
            int vehicles = Integer.parseInt(tokenizer.nextToken());

            // Creating the output key-value pair
            regionDay.set(baseNumber + "," + dayOfWeek);
            tripsVehicles.set(trips + "," + vehicles);

            context.write(regionDay, tripsVehicles);
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int totalTrips = 0;
            int totalVehicles = 0;

            // Calculating the sum of trips and vehicles for the same region and day
            for (Text value : values) {
                String[] parts = value.toString().split(",");
                int trips = Integer.parseInt(parts[0]);
                int vehicles = Integer.parseInt(parts[1]);

                totalTrips += trips;
                totalVehicles += vehicles;
            }

            // Creating the output value
            String outputValue = totalTrips + "," + totalVehicles;
            result.set(outputValue);

            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "UBERStudent20201051");
        job.setJarByClass(UBERStudent20201051.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
      
        FileSystem.get(job.getConfiguration()).delete( new Path(args[1]), true);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
