import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Problem3_4 {

    public static class joinMapper
            extends Mapper<Object, Text, Text, Text> {
        public static final String PROJECT_CONF_KEY = "PROBLEM34";
        private Map<String, String> customerCache = new HashMap<>();
        private Text output_key = new Text();
        private Text output_value = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException{
            try{
                URI[] cachedFiles = context.getCacheFiles();
                final String thisCache = context.getConfiguration().get(PROJECT_CONF_KEY);
                for (URI cacheFile: cachedFiles)
                {
                    Path path = new Path(cacheFile);
                    if(path.getName().equals(thisCache))
                    {
                        BufferedReader br = new BufferedReader(new FileReader(path.getName()));
                        String line;
                        line = br.readLine();
                        while (line!= null)
                        {

                            String key = line.substring(0, line.indexOf(","));
                            String value = line.substring(line.indexOf(",") + 1);
                            customerCache.put(key, value);
                            line = br.readLine();
                        }

                    }
                }
            }catch (Exception e)
            {
                System.out.println("setup having a problem" + e.toString());
            }


        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            try{
                String[] str = value.toString().split(",");
                String customerID = str[1];
                if (customerCache.containsKey(customerID))
                {
                    String customerInfo = customerCache.get(customerID);
                    String[] cusInfoArray = customerInfo.split(",");
                    String countryCode = cusInfoArray[3];
                    String transTotal = str[2];
                    output_key.set(countryCode);
                    output_value.set(String.join(",", customerID, transTotal));
                    context.write(output_key, output_value);
                }

            }
            catch (Exception e)
            {
                System.out.println("map having problem" + e.toString());
            }
        }
    }

/***
 Try to implement combiner. Like Map produces countryCode + id, and combiner reduces one person's
 transaction records.However, I think the combiner is not working since the partitioner is
 happening before combiner. And this process will generate countryCode + id as key not countryCode.
***/


//    public static class InfoCombiner
//            extends Reducer<Text, Text, Text, Text> {
//        private Text outputKey = new Text();
//        private Text outputValue = new Text();
//        @Override
//        public void reduce(Text key, Iterable<Text> values,
//                           Context context
//        ) throws IOException, InterruptedException {
//            try {
//                double minTransTotal = 1000;
//                double maxTransTotal = 10;
//                String[] customerInfo = key.toString().split(",");
//                String customerID = customerInfo[0];
//                String countryCode = customerInfo[1];
//                for (Text val : values) {
//                    Double trans = Double.parseDouble(val.toString());
//                    minTransTotal = Math.min(minTransTotal, trans);
//                    maxTransTotal = Math.max(maxTransTotal, trans);
//                }
//                outputKey.set(countryCode);
//                String output = String.join(",", customerID, String.valueOf(minTransTotal), String.valueOf(maxTransTotal));
//                outputValue.set(output);
//                context.write(outputKey, outputValue);
//            }catch (Exception e)
//            {
//                System.out.println("combiner having problem" + e.toString());
//            }
//        }
//    }


    // Reducer of this Map-Reduce Job
    public static class joinReducer
            extends Reducer<Text, Text, Text, NullWritable> {
        private Text outputKey = new Text();
        @Override
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            try{
                Map<String, String> customerPeople = new HashMap<>();
                double minTransTotal = 1000;
                double maxTransTotal = 10;
                String countryCode = key.toString();
                for (Text val : values) {
                    String[] str = val.toString().split(",");
                    if (!customerPeople.containsKey(str[0]))
                    {
                        customerPeople.put(str[0], "1");
                    }
                    minTransTotal = Math.min(minTransTotal, Double.parseDouble(str[1]));
                    maxTransTotal = Math.max(maxTransTotal, Double.parseDouble((str[1])));
                }
                String output = String.join(",", countryCode, String.valueOf(customerPeople.size()), String.valueOf(minTransTotal), String.valueOf(maxTransTotal));
                outputKey.set(output);
                context.write(outputKey, NullWritable.get());
            }catch (Exception e)
            {
                System.out.println("reducer having problem");
            }
        }

    }



    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "3_4");
        job.setJarByClass(Problem3_4.class);
        job.setMapperClass(joinMapper.class);
        job.setReducerClass(joinReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        // cache file for customer
        Path customerPath = new Path(args[0]);
        job.addCacheFile(customerPath.toUri());
        job.getConfiguration().set(joinMapper.PROJECT_CONF_KEY, customerPath.getName());
        FileInputFormat.setInputPaths(job, args[1]);
        FileSystem fs = FileSystem.getLocal(conf);
        Path p = new Path(args[2]);
        if(fs.exists(p)){
            fs.delete(p, true);
        }
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}