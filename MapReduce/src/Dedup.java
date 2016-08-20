import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 数据去重
 * @author caixiaocong
 *
 */
public class Dedup extends Configured implements Tool{

	public static final Log log = LogFactory.getLog(Dedup.class);

	//Map
	public static class MyMapper extends Mapper<LongWritable,Text,Text,Text>{
		
		@Override
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			context.write(value,new Text(""));
		}
		
	}
	
	//Reduce
	public static class MyReducer extends Reducer<Text,Text,Text,Text>{
		
		@Override
		public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
			context.write(key, new Text(""));
		}
		
	}
	
	//Run
	public int run(String[] args) throws Exception {
		
		Configuration conf = new Configuration();  
        Job job = Job.getInstance(conf,"Dedup");
        
        job.setJarByClass(Dedup.class);  
        job.setMapperClass(MyMapper.class);  
        job.setReducerClass(MyReducer.class);
      
        job.setOutputKeyClass(Text.class);  
        job.setOutputValueClass(Text.class);  
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
			"./target/input/3",
			"./target/output/3"
		};
		int ret = ToolRunner.run(new Dedup(), arg);
			
		log.info("MapReduce Finished.");
		System.exit(ret);
	}
	
}
