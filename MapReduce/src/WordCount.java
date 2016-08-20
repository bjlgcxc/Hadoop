import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * WordCount
 * @author caixiaocong
 *
 */
public class WordCount extends Configured implements Tool {

	public static final Log log = LogFactory.getLog(WordCount.class);
	
	
	//Mapper
	public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		@Override
		public void setup(Context context){}
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer token = new StringTokenizer(line);
			while (token.hasMoreTokens()) {
				context.write(new Text(token.nextToken()), new IntWritable(1));
			}
		}		
		
		@Override
		public void cleanup(Context context){}
	}

	
	//Reducer
	public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {	
		@Override
		public void setup(Context context){}
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			context.write(key, new IntWritable(sum));
		}
		
		@Override
		public void cleanup(Context context){}
	}
	
	
	//Partitioner
	public static class MyPartitioner extends Partitioner<Text, IntWritable>{

		@Override
		public int getPartition(Text key, IntWritable value, int numPartitions) {
			return 0;
		}
		
	} 
	

	//Run
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();  
        Job job = Job.getInstance(conf,"WordCount");
        
        job.setJarByClass(WordCount.class);  
        job.setMapperClass(MyMapper.class);  
        job.setReducerClass(MyReducer.class);
        job.setCombinerClass(MyReducer.class);
        job.setPartitionerClass(MyPartitioner.class);
        job.setNumReduceTasks(2);
        
        job.setOutputKeyClass(Text.class);  
        job.setOutputValueClass(IntWritable.class);  
        job.setInputFormatClass(TextInputFormat.class);  
        job.setOutputFormatClass(TextOutputFormat.class);  
  
        FileInputFormat.addInputPath(job, new Path(args[0]));  
		Path outdir = new Path(args[1]);
		FileSystem fs = outdir.getFileSystem(conf);
		fs.delete(outdir, true);
		FileOutputFormat.setOutputPath(job, outdir);

		return job.waitForCompletion(true) ? 0 : 1;  
	}
	
	
	public static void main(String[] args) throws Exception {
		String arg[] = new String[]{
			"./target/input/1",
			"./target/output/1"
		};
		int ret = ToolRunner.run(new WordCount(), arg);
		
		log.info("MapReduce Finished.");
		System.exit(ret);
	}

}
