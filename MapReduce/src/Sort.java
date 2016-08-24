import java.io.IOException;
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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * ≈≈–Ú
 * @author caixiaocong
 *
 */
public class Sort extends Configured implements Tool{

	public static final Log log = LogFactory.getLog(Sort.class);
	
	//Map
	public static class MyMapper extends Mapper<LongWritable,Text,IntWritable,IntWritable>{
		
		@Override
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			int number = Integer.parseInt(value.toString());
			context.write(new IntWritable(number),new IntWritable(1));
		}
		
	}
	
	//Reduce
	public static class MyReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>{
		
		private static IntWritable linenum = new IntWritable(1); 
		
		@Override
		@SuppressWarnings("unused")
		public void reduce(IntWritable key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{
			for(IntWritable value:values){
				context.write(linenum, key);
				linenum = new IntWritable(linenum.get()+1);
			}
		}
		
	}
		
	//Run
	public int run(String[] args) throws Exception {
		
		Configuration conf = new Configuration();  
        Job job = Job.getInstance(conf,"Sort");
        
        job.setJarByClass(Sort.class);  
        job.setMapperClass(MyMapper.class);  
        job.setReducerClass(MyReducer.class);
      
        job.setOutputKeyClass(IntWritable.class);  
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
			"./target/input/4",
			"./target/output/4"
		};
		int ret = ToolRunner.run(new Sort(), arg);
			
		log.info("MapReduce Finished.");
		System.exit(ret);
	}
	
}
