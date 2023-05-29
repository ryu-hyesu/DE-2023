import java.util.PriorityQueue;
import java.util.Collections;
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
    public String category;
    public double averageRating;
    
    public Emp(String category, double averageRating) {
        this.category = category;
        this.averageRating = averageRating;
    }
    
    public String getString()
    {
        return category + " " + averageRating;
    }
}

class EmpComparator implements Comparator<Emp> {
    public int compare(Emp x, Emp y) {
        if ( x.averageRating > y.averageRating ) return 1;
        if ( x.averageRating < y.averageRating ) return -1;
        return 0;
    }

    
}

public class YouTubeStudent20201051
{
	public static void insertEmp(PriorityQueue<Emp> q, String category, double averageRating, int topK) {
        Emp emp_head = q.peek();
        if (q.size() < topK || emp_head.averageRating < averageRating) {
            Emp emp = new Emp(category, averageRating);
            q.add(emp);
            if (q.size() > topK) q.remove();
        }
    }
  
    // error!
  public static class TopKMapper extends Mapper<Object, Text, Text, DoubleWritable> {

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
       
	Text category = new Text();
	DoubleWritable rating = new DoubleWritable();
    	
	String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line, "|");
	

	String data1 = tokenizer.nextToken();
	String data2 = tokenizer.nextToken();
	String data3 = tokenizer.nextToken();
	String cate = tokenizer.nextToken().trim();
	String data4 = tokenizer.nextToken();
	String data5 = tokenizer.nextToken();
	Double score = Double.parseDouble(tokenizer.nextToken().trim());

	category.set(cate);
	rating.set(score);

	context.write(category, rating); // error!!
     
    }
      
}

  public static class TopKReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    private PriorityQueue<Emp> queue ;
    private Comparator<Emp> comp = new EmpComparator();
    private int topK;
	  
	Text reduce_key = new Text();
	DoubleWritable o_value = new DoubleWritable();  

    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        topK = conf.getInt("topK", -1);
        queue = new PriorityQueue<Emp>( topK , comp);
    }

    public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {
        double sum = 0;
        int count = 0;

        for (DoubleWritable value : values) {
            sum += value.get();
            count++;
        }

        double average = sum / count; // 평균
        
        insertEmp(queue, key.toString(), average, topK);
        
    }

      
    protected void cleanup(Context context) throws IOException, InterruptedException {
        while( queue.size() != 0 ) {
        Emp emp = (Emp) queue.remove();
		
        reduce_key.set(emp.category);
	o_value.set(emp.averageRating);
	context.write(reduce_key, o_value);
      }
    }
}

  public static void main(String[] args) throws Exception {
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
