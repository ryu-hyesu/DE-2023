import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class MatrixAdd{
	
	// mapper
	// <map input key type, map input value type, map output key type, map output value type>
	public static class MatrixAddMapper extends Mapper<Object, Text, Text, IntWritable>{
		private final static IntWritable i_value = new IntWritable(1);
		private Text word = new Text();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			StringTokenizer itr = new StringTokenizer(value.toString());
			int row_id = Integer.parseInt( itr.nextToken().trim() );
			int col_id = Integer.parseInt( itr.nextToken().trim() );
			int m_value = Integer.parseInt( itr.nextToken().trim() );

			word.set( row_id + "," + col_id );
			i_value.set( m_value );

			context.write( word, i_value); // key-value pair 만들고 emit
		}
	}

	public static class MatrixAddReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		private IntWritable result = new IntWritable();
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
			int sum = 0;

			for( IntWritable val : values)
				sum += val.get();

			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception
        {
                Configuration conf = new Configuration();
                Job job = new Job(conf, "MatrixAdd");

                job.setJarByClass(MatrixAdd.class);
                job.setMapperClass(MatrixAddMapper.class);
                job.setCombinerClass(MatrixAddReducer.class);
                job.setReducerClass(MatrixAddReducer.class);

                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(IntWritable.class);

                job.setInputFormatClass(TextInputFormat.class);
                job.setOutputFormatClass(TextOutputFormat.class);

                FileInputFormat.addInputPath(job, new Path(args[0]));
                FileOutputFormat.setOutputPath(job, new Path(args[1]));

                FileSystem.get(job.getConfiguration()).delete( new Path(args[1]), true);

                job.waitForCompletion(true);
        }


}
