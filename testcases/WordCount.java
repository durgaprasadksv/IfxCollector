import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCount {

	public static class Map extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			
			long freeMemory = Runtime.getRuntime().freeMemory();
			long maximumMemory = Runtime.getRuntime().maxMemory();
			long totalMemory = Runtime.getRuntime().totalMemory();
			
			System.out.printf("Start freeMemory is: %d \n", freeMemory);
			System.out.printf("Start maximumMemory is: %d \n", maximumMemory);
			System.out.printf("Start totalMemory is: %d \n", totalMemory);
			System.out.printf("Start tatol length is: %s \n", line.length());
			System.out.printf("Start tatol size is: %s \n", line.getBytes());
			
			for (int i = 0; i < 1000; i++ ){
				line = line + line;				
			}
			
			 freeMemory = Runtime.getRuntime().freeMemory();
			 maximumMemory = Runtime.getRuntime().maxMemory();
			 totalMemory = Runtime.getRuntime().totalMemory();
			 
			System.out.printf("Start freeMemory is: %d \n", freeMemory);
			System.out.printf("Start maximumMemory is: %d \n", maximumMemory);
			System.out.printf("Start totalMemory is: %d \n", totalMemory);			 
			System.out.printf("Finished tatol length is: %s \n", line.length());
			System.out.printf("Finished tatol size is: %s \n", line.getBytes());
			
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				context.write(word, one);
			}
		}
	}

	public static class Reduce extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "wordcount");
		job.setJarByClass(WordCount.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		job.waitForCompletion(true);
	}

}
