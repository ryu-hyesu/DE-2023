import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapred.lib.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class YouTubeStudent20201051
{
  public class CategoryRating {
    private String category;
    private double averageRating;

    public CategoryRating(String category, double averageRating) {
        this.category = category;
        this.averageRating = averageRating;
    }

    public String getCategory() {
        return category;
    }

    public double getAverageRating() {
        return averageRating;
    }

    @Override
    public String toString() {
        return category + " " + averageRating;
    }
}
  
  public static class TopKMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    private Text category = new Text();
    private DoubleWritable rating = new DoubleWritable();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("|");

        if (fields.length >= 7) {
            String category = fields[3];
            double rating = Double.parseDouble(fields[6]);

            context.write(new Text(category), new DoubleWritable(rating));
        }
    }
}

  public static class TopKReducer extends Reducer<Text, DoubleWritable, Text, NullWritable> {
    private PriorityQueue<CategoryRating> topCategories;
    private int topK;

    protected void setup(Context context) {
        topCategories = new PriorityQueue<>(topK, Comparator.comparingDouble(CategoryRating::getAverageRating));
        topK = context.getConfiguration().getInt("topK", 10);
    }

    public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {
        double sum = 0;
        int count = 0;

        for (DoubleWritable value : values) {
            sum += value.get();
            count++;
        }

        double average = sum / count;
        CategoryRating categoryRating = new CategoryRating(key.toString(), average);

        topCategories.add(categoryRating);

        // Keep only top K categories in the PriorityQueue
        if (topCategories.size() > topK) {
            topCategories.poll();
        }
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        // Emit the top K categories in descending order of average rating
        while (!topCategories.isEmpty()) {
            CategoryRating categoryRating = topCategories.poll();
            context.write(new Text(categoryRating.toString()), NullWritable.get());
        }
    }
}

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    
    if (otherArgs.length != 3) {
        System.err.println("Usage: TopK <inputPath> <outputPath> <topK>");
        System.exit(2);
    }

    String inputPath = otherArgs[0];
    String outputPath = otherArgs[1];
    int topK = Integer.parseInt(otherArgs[2]);

    conf.setInt("topK", topK);

    Job job = Job.getInstance(conf, "TopK");
    job.setJarByClass(YouTubeStudent20201051.class);

    // Mapper 설정
    job.setMapperClass(TopKMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(DoubleWritable.class);

    // Reducer 설정
    job.setReducerClass(TopKReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);

    // 입력 및 출력 경로 설정
    FileInputFormat.addInputPath(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    // 출력 디렉토리 삭제
    FileSystem.get(conf).delete(new Path(outputPath), true);
    

    System.exit(job.waitForCompletion(true) ? 0 : 1);
}

 
}
