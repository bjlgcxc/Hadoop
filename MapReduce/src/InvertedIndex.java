import java.io.IOException;
import java.util.StringTokenizer;
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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class InvertedIndex extends Configured implements Tool{

	public static final Log log = LogFactory.getLog(InvertedIndex.class);
	
	
	//Mapper
	public static class MyMapper extends Mapper<LongWritable,Text,Text,Text>{
		
		@Override
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			
			String line = value.toString();
			StringTokenizer tokens = new StringTokenizer(line);
			
			//获取输入文件名
			FileSplit fileSplit = (FileSplit)context.getInputSplit();
			String split[] = fileSplit.getPath().toString().split("/");
			
			while(tokens.hasMoreTokens()){
				context.write(new Text(split[split.length-1] + ":" + tokens.nextToken()), new Text("1"));
			}

		}
		
	}
	
	
	//Combiner
	public static class MyCombiner extends Reducer<Text,Text,Text,Text>{
		
		@Override
		public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
			System.out.println(key);
			String tokens[] = key.toString().split(":");
			int sum = 0;
			for(Text value:values){
				sum += Integer.parseInt(value.toString());
			}
			context.write(new Text(tokens[1]), new Text(tokens[0] + ":" + sum));
			System.out.println(tokens[0] + ":" + sum);
			System.out.println("11");
			System.out.println(tokens[1]);
		}
		
	}
	
	
	//Reducer
	public static class MyReducer extends Reducer<Text,Text,Text,Text>{
		
		@Override
		public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
			String str = "";
			for(Text value:values){
				str += value + ";";
			}
			context.write(key,new Text(str));
		}
		
	}
	
	
	//run
	public int run(String[] args) throws Exception {
		
		Configuration conf = new Configuration();  
        Job job = Job.getInstance(conf,"InvertedIndex");
        
        job.setJarByClass(InvertedIndex.class);  
        job.setMapperClass(MyMapper.class);  
        job.setCombinerClass(MyCombiner.class);
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
			"./target/input/file1.txt",
			"./target/output/2"
		};
		int ret = ToolRunner.run(new InvertedIndex(), arg);
			
		log.info("MapReduce Finished.");
		System.exit(ret);
	}
	
}
