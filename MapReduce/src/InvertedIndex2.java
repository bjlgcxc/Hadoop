import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;
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

/**
 * 倒排索引(词频高的在前)
 * @author caixiaocong
 *
 */
public class InvertedIndex2 extends Configured implements Tool{

	public static final Log log = LogFactory.getLog(InvertedIndex2.class);
	
	//Mapper
	public static class MyMapper extends Mapper<LongWritable,Text,Text,Text>{
		
		HashMap<String,Integer> map = new HashMap<String,Integer>();
		
		@Override
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			
			String line = value.toString();
			StringTokenizer tokens = new StringTokenizer(line);
			
			//获取输入文件名
			FileSplit fileSplit = (FileSplit)context.getInputSplit();
			String fileName = fileSplit.getPath().getName();
			
			while(tokens.hasMoreTokens()){
				String term = tokens.nextToken();
				if(map.containsKey(term)){
					map.put(term,map.get(term)+1);
				}
				else{
					map.put(term,new Integer(1));
				}
			}
			for(Entry<String,Integer> entry:map.entrySet()){
				context.write(new Text(entry.getKey() + " " + entry.getValue()), new Text(fileName));
			}
		
		}
		
	}
	
	
	//Reducer
	public static class MyReducer extends Reducer<Text,Text,Text,Text>{
		
		String current = "";
		String str = "";
		
		@Override
		public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
			String term = key.toString().split(" ")[0];
			String freq = key.toString().split(" ")[1];
			if(!current.equals(term)){
				if(!str.equals("")){
					context.write(new Text(term),new Text(str));
					str = "";
				}
				current = term;
			}
			else{
				for(Text value:values){
					str = value.toString() + ":" + freq + ";" + str ;
				}	
			}
			
		}
		
	}
	
	
	//run
	public int run(String[] args) throws Exception {
		
		Configuration conf = new Configuration();  
        Job job = Job.getInstance(conf,"InvertedIndex2");
        
        job.setJarByClass(InvertedIndex2.class);  
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
			"./target/input/2",
			"./target/output/2"
		};
		int ret = ToolRunner.run(new InvertedIndex2(), arg);
			
		log.info("MapReduce Finished.");
		System.exit(ret);
	}
	
}
