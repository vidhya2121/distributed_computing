import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Task1 {

  // add code here

  public static class Task1Mapper extends Mapper<Object, Text, Text, Text>
  {
	  
	   
	   public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException 
					{

            String[] arr = value.toString().split(",", -1);
            
            StringBuilder result = new StringBuilder();
            int max = 1;
            for(int i = 1; i < arr.length; i++){
                // check if this is empty string
                if (arr[i].length() != 0){
                    if(Integer.parseInt(arr[i]) > max){
                        //set new max
                        max = Integer.parseInt(arr[i]);
                        //clear builder
                        result.setLength(0);
                        //add index to builder
                        result.append(i);
                    }
                    else if(Integer.parseInt(arr[i]) == max){
                        //add a comma and then the index to the result
                        result.append(",").append(i);
                    }
                }
            }
						
            

						context.write(new Text(arr[0]), new Text(result.toString()));
            //context.write(k, v);
            //System.out.println("Print in Mapper count %d",count);
					}
	  
  }


    
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapreduce.output.textoutputformat.separator", ",");
    
    Job job = Job.getInstance(conf, "Task1");
    job.setJarByClass(Task1.class);

    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    
    // add code here
    job.setMapperClass(Task1Mapper.class);
    //job.setCombinerClass(Task1Mapper.class);
    //job.setReducerClass(Task1Mapper.class);
	  job.setNumReduceTasks(0);
	
	  job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);


    TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
    TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
