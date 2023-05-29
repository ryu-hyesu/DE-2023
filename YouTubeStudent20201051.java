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
        return category + "," + averageRating;
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
  
    // 오류남
  public static class TopKMapper extends Mapper<Object, Text, Text, DoubleWritable> {
    private Text category = new Text();
    private DoubleWritable rating = new DoubleWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        	
	    String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line, "|");

		String id = tokenizer.nextToken();
		String title = tokenizer.nextToken(); // 오류남!!!!
		String genre = tokenizer.nextToken(); // 장르
		String name = tokenizer.nextToken();
		String num1 = tokenizer.nextToken();
		String num2 = tokenizer.nextToken();
		Double score = Double.parseDouble(tokenizer.nextToken()); // 점수
        
            
           
	    
	    category.set(title);
	    rating.set(score);

            context.write(category, rating);
     
    }
      
}

  public static class TopKReducer extends Reducer<Text, DoubleWritable, Text, NullWritable> {
    private PriorityQueue<Emp> queue ;
    private Comparator<Emp> comp = new EmpComparator();
    private int topK;

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
        context.write( new Text( emp.getString() ), NullWritable.get() );
      }
    }
}

  public static void main(String[] args) throws Exception {
  	Configuration conf = new Configuration();
	String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

	int topK = 2;

	if (otherArgs.length != 2) {
		System.err.println("Usage: TopK <in> <out>");   System.exit(2);
	}

	conf.setInt("topK", topK);
	Job job = new Job(conf, "TopK");

	job.setJarByClass(YouTubeStudent20201051.class);
	job.setMapperClass(TopKMapper.class);
	job.setReducerClass(TopKReducer.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(NullWritable.class);
	FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

	FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
	System.exit(job.waitForCompletion(true) ? 0 : 1);  
  }

 
}
