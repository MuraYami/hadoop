package kmercount1;

//Import the necessary libraries
import java.lang.Math;
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

public class KmerCount {

public static class KmerMapper 
	extends Mapper<Object, Text, Text, IntWritable>{

private static final int K = 3;
private final static IntWritable one = new IntWritable(1);
private Text kmer = new Text();

public void map(Object key, Text value, Context context) 
		 throws IOException, InterruptedException {
 String value2;
 value2 = "";

 int m = 0;
 while (m<value.toString().length()) {
	   int x = Math.min(m + 5*K + K-1, value.toString().length());
	   value2 += value.toString().substring(m, x) + " ";
	   
	   m = x;
 }
 
 StringTokenizer itr = new StringTokenizer(value2);
 while (itr.hasMoreTokens()) {
   String token = itr.nextToken();
   if (token.length() >= K) {
     for (int i = 0; i <= token.length() - K; i++) {
       String sub = token.substring(i, i + K);
       kmer.set(sub);
       context.write(kmer, one);
     }
   }
 }
}
}

public static class IntSumReducer 
	extends Reducer<Text,IntWritable,Text,IntWritable> {
private IntWritable result = new IntWritable();
public void reduce(Text key, Iterable<IntWritable> values, Context context) 
		 throws IOException, InterruptedException {
 int sum = 0;
 for (IntWritable val : values) {
   sum += val.get();
 }
 result.set(sum);
 context.write(key, result);
}
}

//Define the main method
public static void main(String[] args) throws Exception {
// Create a Configuration object
Configuration conf = new Configuration();
// Create a Job object with conf and a name
Job job = Job.getInstance(conf, "kmer count");
// Set the jar by finding where this class came from
job.setJarByClass(KmerCount.class);
// Set the mapper class to KmerMapper
job.setMapperClass(KmerMapper.class);
// Set the combiner class to IntSumReducer
job.setCombinerClass(IntSumReducer.class);
// Set the reducer class to IntSumReducer
job.setReducerClass(IntSumReducer.class);
// Set the output key class to Text
job.setOutputKeyClass(Text.class);
// Set the output value class to IntWritable
job.setOutputValueClass(IntWritable.class);
// Set the input path to the first argument
FileInputFormat.addInputPath(job, new Path(args[0]));
// Set the output path to the second argument
FileOutputFormat.setOutputPath(job, new Path(args[1]));
// Wait for completion and exit with status code
System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}
