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

  public static class ReduceSideJoinMapper extends Mapper<Object, Text, Text, DoubleWritable>
  {
    private Text category = new Text();
    private DoubleWritable rating = new DoubleWritable();
    
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
      String[] fields = value.toString().split("|");
      
      category.set(fields[3]);
      rating.set(Double.parseDouble(fields[6]));
      
      context.write(category, rating);
    }
    
  }
  
  
  public static class ReduceSideJoinReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable>
  {
    private int K;
    private TreeMap<Double, String> categoryRatingsMap;

    public void setup(Context context) {
        Configuration conf = context.getConfiguration();
        K = conf.getInt("K", 1);
        categoryRatingsMap = new TreeMap<>();
    }
    
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, 
    InterruptedException {
      for (Text value : values) {
          String[] fields = value.toString().split("|");
        
          if (fields.length >= 2) {
              String category = fields[0];
              double rating = Double.parseDouble(fields[1]);
              categoryRatingsMap.put(rating, category);
              if (categoryRatingsMap.size() > K) {
                  categoryRatingsMap.remove(categoryRatingsMap.firstKey());
              }
          }
      }
      for (Map.Entry<Double, String> entry : categoryRatingsMap.entrySet()) {
          double rating = entry.getKey();
          String category = entry.getValue();
          context.write(new Text(category), new DoubleWritable(rating));
      }
    }
  }
  
  
 public static void main(String[] args) throws Exception
  {
  
   
  if (otherArgs.length != 3)
  {
    System.err.println("Usage: ReduceSideJoin <in> <out>");
    System.exit(2);
  }
  
  Configuration conf = new Configuration();
  Job job = new Job(conf, "YouTubeStudent20201051");
  job.setJarByClass(YouTubeStudent20201051.class);
  job.setMapperClass(ReduceSideJoinMapper.class);
  job.setReducerClass(ReduceSideJoinReducer.class);
  job.setOutputKeyClass(Text.class);
  job.setOutputValueClass(Text.class);
  FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
  FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
  
   int K = Integer.parseInt(args[2]);
        job.getConfiguration().setInt("K", K);
   
  FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
  System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
