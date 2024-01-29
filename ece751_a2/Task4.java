import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;

public class Task4 {

    // add code here
    public static class SimilarityMapper extends Mapper<Object, Text, Text, IntWritable> {
        HashMap<String, Integer[]> movies = new HashMap<>();
        private IntWritable user = new IntWritable();
        private Text movie_names = new Text();
        StringBuilder name = new StringBuilder();

        protected void setup(Context context) throws IOException, InterruptedException {
            Path[] movieRatings = DistributedCache.getLocalCacheFiles(context.getConfiguration());
            readFile(movieRatings[0]);
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String movie1 = value.toString().split(",", 2)[0];
            Integer[] ratings1 = this.movies.get(movie1);
            for (String movie2 : movies.keySet()) {
                int count = 0;
                if (movie2.compareTo(movie1) > 0) {
                	Integer[] ratings2 = this.movies.get(movie2);
                    for (int x = 0; x < ratings1.length; x++) {
                        if (ratings1[x] == ratings2[x] && ratings1[x] !=0) {
                            count++;
                        }
                    }
                    user.set(count);
                    name.setLength(0);
                    name.append(movie1).append(",").append(movie2);
                    movie_names.set(name.toString());
                    context.write(movie_names, user);
                }
            }

        }

        private void readFile(Path filePath) {
            try {
                BufferedReader bufferedReader = new BufferedReader(new FileReader("file21"));
                String line = null;
                while ((line = bufferedReader.readLine()) != null) {
                    String[] movie = line.split(",", -1);
                    Integer[] ratings = new Integer[movie.length - 1];
                    for (int i = 1; i < movie.length; i++) {
                        ratings[i - 1] = movie[i].length() == 0 ? 0 : Integer.parseInt(movie[i]);
                    }
                    this.movies.put(movie[0], ratings);
                }
            } catch (IOException ex) {
                System.err.println("Exception while reading stop words file: " + ex.getMessage());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", ",");

        Job job = Job.getInstance(conf, "Task4");
        job.setJarByClass(Task4.class);
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        // add code here

        job.setMapperClass(SimilarityMapper.class);
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.addCacheFile(new URI(otherArgs[0] + "#file21"));

        TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
        TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}



