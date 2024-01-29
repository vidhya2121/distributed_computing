import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Task2 {

	// add code here
	public static class Task2Mapper extends Mapper<Object, Text, NullWritable, IntWritable> {

		private final static IntWritable one = new IntWritable(1);

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] movReview = value.toString().split(",", -1);
			int count = 0;
			for (int i = 1; i < movReview.length; i++) {

				if (!movReview[i].equals("")) {
					count++;
				}
			}
			one.set(count);
			context.write(NullWritable.get(), one);
		}

	}

	public static class Task2Reducer extends Reducer<NullWritable, IntWritable, NullWritable, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(NullWritable key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(NullWritable.get(), result);
		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("mapreduce.output.textoutputformat.separator", ",");

		Job job = Job.getInstance(conf, "Task2");
		job.setJarByClass(Task2.class);

		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: ReviewCount <in> <out>");
			System.exit(2);
		}

		// add code here
		job.setMapperClass(Task2Mapper.class);
		job.setCombinerClass(Task2Reducer.class);
		job.setReducerClass(Task2Reducer.class);
		job.setNumReduceTasks(1);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(IntWritable.class);

		TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
		TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

