package hw1;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.Iterator;
import java.net.URI;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.*;

public class ucache extends Configured implements Tool {
	
	
		public static class WordMapper 
			extends Mapper<Object, Text, Text, IntWritable> {
			
			ArrayList<String> search_list = new ArrayList<String>(); 
			
			private Text word = new Text();
			private final static IntWritable one = new IntWritable(1);
	
			protected void setup(Mapper<Object, Text, Text, IntWritable>.Context context)
					throws IOException, InterruptedException {
				
				
				System.out.println("to start setup");    // debug statement
			        try {
			        	System.out.println("in set up"); // to debug, whether the pattern.txt has been loaded
			        	
			        	Configuration conf = context.getConfiguration();
			            FileSystem fs = FileSystem.get(conf);
			            FSDataInputStream in = fs.open(new Path("/user/hadoop/input/word-patterns.txt")); // read file in cache
			            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
			            
						String line=null;
						System.out.println("got file");   // this statement will be shown in logs
						while ((line = reader.readLine()) != null) {
							StringTokenizer tokenizer = new StringTokenizer(line);
							String token = null;
							while (tokenizer.hasMoreTokens()) {
								token = tokenizer.nextToken();
								search_list.add(token);
							}
						}
						reader.close();
					} catch (FileNotFoundException e) {
						System.err.println("Caught exception while opening cached file : "
								+ StringUtils.stringifyException(e));
					} catch (IOException e) {
						System.err.println("Caught exception while reading cached file : "
								+ StringUtils.stringifyException(e));
					}
				
				super.setup(context);
	
			}
			
		
		public void map(Object key, Text value, Context contex) throws IOException,
				InterruptedException {
			
			StringTokenizer wordList = new StringTokenizer(value.toString());
			String currentToken = null;
			while (wordList.hasMoreTokens()) {
				currentToken = wordList.nextToken();
				
				if (search_list.contains(currentToken)) {
					word.set(currentToken);
					contex.write(word, one);
				}
			}
		}		
	}


		public static class IntReducer 
	    				extends Reducer<Text,IntWritable,Text,IntWritable> {
			private IntWritable result = new IntWritable();
	
			public void reduce(Text key, Iterable<IntWritable> values,Context context) 
					throws IOException, InterruptedException {
				int sum = 0;
				   for (IntWritable val : values) {
				     sum += val.get();
				   }
				   result.set(sum);
				   context.write(key, result);
			 }
		}
	
	

		public int run(String[] args) throws Exception {
							
			Configuration conf = getConf();
							
			Job job = Job.getInstance(conf);
	
			job.setJobName("using cache");
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
	
			job.setMapperClass(WordMapper.class);
			job.setReducerClass(IntReducer.class);
	
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			
			
			List<String> otherArgs = new ArrayList<String>();
	
			for (int i = 0; i < args.length; i++) {
				System.out.println(args[i]);
				if ("-slist".equals(args[i])) {
					String cacheFile = args[++i];
					job.addCacheFile(new Path(cacheFile).toUri()); 
				} else {
					otherArgs.add(args[i]);
				}
				System.out.println("OtherArgs :" + otherArgs);
			}
			
			FileInputFormat.setInputPaths(job, new Path(otherArgs.get(2)));
			FileOutputFormat.setOutputPath(job, new Path(otherArgs.get(3)));
			 
			job.setJarByClass(ucache.class);
			
			return job.waitForCompletion(true) ? 0 : 1;	
		}

	
		public static void main(String[] args) throws Exception {
			
			int hh = ToolRunner.run(new Configuration(), new ucache(), args); 
			System.exit(hh);
	
		}
}