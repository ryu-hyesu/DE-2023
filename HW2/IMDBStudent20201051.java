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
class Emp {
    public String title;
    public double rate;
    
    public Emp(String title, double rate) {
        this.title = title;
        this.rate = rate;
    }
    
    public String getString()
    {
        return title + "|" + rate;
    }
}

class EmpComparator implements Comparator<Emp> {
    public int compare(Emp x, Emp y) {
        if ( x.rate > y.rate ) return 1;
        if ( x.rate < y.rate ) return -1;
        return 0;
    }

    
}
  

  
public class IMDBStudent20201051
{
  public static void insertEmp(PriorityQueue<Emp> q, String title, double rate, int topK) {
        Emp emp_head = q.peek();
        if (q.size() < topK || emp_head.rate < rate) {
            Emp emp = new Emp(title, rate);
            q.add(emp);
            if (q.size() > topK) q.remove();
        }
    }

  public static class ReduceSideJoinMapper extends Mapper<Object, Text, Text, Text>
  {
    boolean fileA = true;
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
      StringTokenizer itr = new StringTokenizer(value.toString(), "::");
      Text outputKey = new Text();
      Text outputValue = new Text();
      String joinKey;
      String o_value;
      
      if( fileA ) { // moive
        String id = itr.nextToken().trim(); // movie id
        String title = itr.nextToken().trim(); // moive title(years)
        itr = new StringTokenizer(itr.nextToken().trim(), "|"); // movie category
        
        while( itr.hasMoreTokens() ) {
          if( itr.nextToken().equals("Fantasy") ) {
              joinKey = id;
              o_value = "A," + title;
              break;
          }
        }

      }
      else { // rating
        itr.nextToken(); // id key
        joinKey = itr.nextToken().trim(); // movie id
        o_value += "B," + itr.nextToken().trim(); // rating
      }
      
      outputKey.set( joinKey );
      outputValue.set( o_value );
      context.write( outputKey, outputValue );
    }
    
    protected void setup(Context context) throws IOException, InterruptedException
    {
      String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
      
      if ( filename.indexOf( "movie.dat" ) != -1 ) fileA = true;
      else fileA = false;
     }
  }
  
  
  public static class ReduceSideJoinReducer extends Reducer<Text,Text,Text,DoubleWritable>
  {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, 
    InterruptedException {
      Text reduce_key = new Text();
      Text reduce_result = new Text();
      String description = "";
      ArrayList<String> buffer = new ArrayList<String>();
      
      
      int sum = 0;
      int count = 0;
      String title;
      
      for (Text val : values) {
        String file_type;
        
        StringTokenizer itr = new StringTokenizer(val.toString(), ",");
        file_type = itr.nextToken().trim();
        
        if( file_type.equals( "B" ) ) {
          sum += Integer.parseInt(itr.nextToken().trim());
          count++;
        }
        else { // A일때 title이 key.
          title = itr.nextToken();
        }
      }
      
      double average = sum / count; 
      
      insertEmp(queue, key.title, average, topK);
    }
    
    protected void cleanup(Context context) throws IOException, InterruptedException {
        while( queue.size() != 0 ) {
        Emp emp = (Emp) queue.remove();
		
        context.write(new Text(emp.title), new DoubleWritable(emp.rate));
      }
    }
  }
  
  
 public static void main(String[] args) throws Exception
  {
  Configuration conf = new Configuration();
	String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

	if (otherArgs.length != 3) {
		System.err.println("Usage: TopK <in> <out>");   System.exit(2);
	}
	  
	int topK = Integer.parseInt(otherArgs[2]);
	conf.setInt("topK", topK);
	Job job = new Job(conf, "TopK");

	job.setJarByClass(YouTubeStudent20201051.class);
	job.setMapperClass(TopKMapper.class);
	job.setReducerClass(TopKReducer.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(DoubleWritable.class);
	// job.setMapOutputValueClass(DoubleWritable.class);

	FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

	FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
	System.exit(job.waitForCompletion(true) ? 0 : 1);  
  }
}
  

  


