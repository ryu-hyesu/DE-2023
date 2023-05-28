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
      String[] fields = line.split("|");
      
      category.set(fields[3]);
      rating.set(Double.parseDouble(fields[6]));
      
      context.write(category, rating);
    }
    
  }
  
  
  public static class ReduceSideJoinReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable>
  {
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, 
    InterruptedException {
      double sum = 0;
      int count = 0;
      for (DoubleWritable val : values) {
          sum += val.get();
          count++;
      }
      double average = sum / count;
      avgRating.set(average);
      context.write(key, avgRating);
    }
  }
  
  
 public static void main(String[] args) throws Exception
  {
  Configuration conf = new Configuration();
  String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
  int topK = 3;
   
  if (otherArgs.length != 2)
  {
    System.err.println("Usage: ReduceSideJoin <in> <out>");
    System.exit(2);
  }
   
  conf.setInt("topK", topK);
  Job job = new Job(conf, "YouTubeStudent20201051");
  job.setJarByClass(YouTubeStudent20201051.class);
  job.setMapperClass(ReduceSideJoinMapper.class);
  job.setReducerClass(ReduceSideJoinReducer.class);
  job.setOutputKeyClass(Text.class);
  job.setOutputValueClass(Text.class);
  FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
  FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
  FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
  System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}