package main;

//Import the necessary libraries
import java.io.IOException;
import java.util.StringTokenizer;
import java.lang.Math;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MeanCompute {
	static final int N = 9; 

public static class Mapper1
  extends Mapper<Object, Text, IntWritable, IntWritable>{

public void map(Object key, Text value, Context context
               ) throws IOException, InterruptedException {
 StringTokenizer itr = new StringTokenizer(value.toString());
 
 while (itr.hasMoreTokens()) {
   int i = (int) (Integer.parseInt(itr.nextToken()) / (int)Math.sqrt(N));
   int xi = Integer.parseInt(itr.nextToken());
   context.write(new IntWritable(i), new IntWritable(xi));
 }
}
}

public static class Reducer1
  extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
private IntWritable result = new IntWritable();

public void reduce(IntWritable key, Iterable<IntWritable> values,
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

public static class Mapper2
extends Mapper<Object, Text, IntWritable, DoubleWritable>{

public void map(Object key, Text value, Context context
             ) throws IOException, InterruptedException {
StringTokenizer itr = new StringTokenizer(value.toString());

while (itr.hasMoreTokens()) {
 itr.nextToken();
 double sumk = Double.parseDouble(itr.nextToken());

 context.write(new IntWritable(0), new DoubleWritable(sumk));
}
}
}

public static class Reducer2
extends Reducer<IntWritable,DoubleWritable,IntWritable,DoubleWritable> {

public void reduce(IntWritable key, Iterable<DoubleWritable> values,
                Context context
                ) throws IOException, InterruptedException {
double sum = 0;
for (DoubleWritable val : values) {
 sum += val.get();
}

double mean = sum/(double)N; 

context.write(key, new DoubleWritable(mean));
}
}

public static void main(String[] args) throws Exception {
Configuration conf = new Configuration();
Job job1 = Job.getInstance(conf, "sum partition compute");
job1.setJarByClass(MeanCompute.class);
job1.setMapperClass(Mapper1.class);
job1.setCombinerClass(Reducer1.class);
job1.setReducerClass(Reducer1.class);
job1.setOutputKeyClass(IntWritable.class);
job1.setOutputValueClass(IntWritable.class);
FileInputFormat.addInputPath(job1, new Path(args[0]));
FileOutputFormat.setOutputPath(job1, new Path(args[1]+"/round1"));
job1.waitForCompletion(true);


Job job2 = Job.getInstance(conf, "mean compute");
job2.setJarByClass(MeanCompute.class);
job2.setMapperClass(Mapper2.class);
job2.setCombinerClass(Reducer2.class);
job2.setReducerClass(Reducer2.class);
job2.setOutputKeyClass(IntWritable.class);
job2.setOutputValueClass(DoubleWritable.class);
FileInputFormat.addInputPath(job2, new Path(args[1]+"/round1"));
FileOutputFormat.setOutputPath(job2, new Path(args[1]+"/final"));
System.exit(job2.waitForCompletion(true) ? 0 : 1);
}
}