package hw1;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class wcdouble {
	
		public static class TokenMapper 
			extends Mapper<Object, Text, Text, IntWritable>{
	 
		 private final static IntWritable one = new IntWritable(1);
		 private Text word = new Text();
		   
			 public void map(Object key, Text value, Context context
							 ) throws IOException, InterruptedException {
			   StringTokenizer word_list = new StringTokenizer(value.toString());
			   
			   String prev = null;
			   String current = null;
			   
			   if (word_list.hasMoreTokens()) {
				   prev = word_list.nextToken();
				}//to prevent several exception such as there exist no words in a line
			   
			   while (word_list.hasMoreTokens()) {   	
				 current = word_list.nextToken();
				 word.set(prev + " " + current);
				 context.write(word, one);
				 prev = current;
			   }
			 }
		  }

		public static class IntReducer 
		    extends Reducer<Text,IntWritable,Text,IntWritable> {
		 private IntWritable result = new IntWritable();
		
		 public void reduce(Text key, Iterable<IntWritable> values, 
		                    Context context
		                    ) throws IOException, InterruptedException {
		   int sum = 0;
		   for (IntWritable val : values) {
		     sum += val.get();
		   }
		   result.set(sum);
		   context.write(key, result);
		 }
		}

		public static void main(String[] args) throws Exception {
		 Configuration conf = new Configuration();
		 String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	
				 
		 if (otherArgs.length < 2) {
		   System.err.println("Usage: wordcount <in> [<in>...] <out>");
		   System.exit(2);
		 }
		 Job job = new Job(conf, "word count double");
		 job.setJarByClass(wcdouble.class);
		 
		 job.setMapperClass(TokenMapper.class);
		 job.setCombinerClass(IntReducer.class);
		 job.setReducerClass(IntReducer.class);
		 job.setOutputKeyClass(Text.class);
		 job.setOutputValueClass(IntWritable.class);
		 
		 for (int i = 0; i < otherArgs.length - 1; ++i) {
		   FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		 }
		 
		 FileOutputFormat.setOutputPath(job,new Path(otherArgs[otherArgs.length - 1]));
		 System.exit(job.waitForCompletion(true) ? 0 : 1);
		 
		}
}

