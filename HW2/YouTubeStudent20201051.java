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

public class Emp {
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

public static class EmpComparator implements Comparator<Emp> {
    public int compare(Emp x, Emp y) {
        if ( x.averageRating > y.averageRating ) return 1;
        if ( x.averageRating < y.averageRating ) return -1;
        return 0;
    }
}

public static void insertEmp(PriorityQueue q, String category, double averageRating, int topK) {
        Emp emp_head = (Emp) q.peek();
        if ( q.size() < topK || emp_head.averageRating < averageRating )
        {
            Emp emp = new Emp(category, averageRating);
            q.add( emp );
            if( q.size() > topK ) q.remove();
        }
}

public class YouTubeStudent20201051
{

  
  public static class TopKMapper extends Mapper<Object, Text, Text, DoubleWritable> {
    private Text category = new Text();
    private DoubleWritable rating = new DoubleWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("|");

        if (fields.length >= 7) {
            String category = fields[3];
            double rating = Double.parseDouble(fields[6]);
            
            context.write(new Text(category), new DoubleWritable(rating));
        }
    }
      
}

  public static class TopKReducer extends Reducer<Text, DoubleWritable, Text, NullWritable> {
    private PriorityQueue<Emp> queue ;
    private Comparator<Emp> comp = new EmpComparator();
    private int topK;

    public void reduce(Text key, Iterable<NullWritable> values, Context context)
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
    }

    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        topK = conf.getInt("topK", -1);
        queue = new PriorityQueue<Emp>( topK , comp);
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
