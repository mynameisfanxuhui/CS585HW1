import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.io.BufferedReader;

import java.io.InputStreamReader;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;


// Author xli14@wpi.edu
public class Problem3_5 {
    public static class TransMapper extends
            Mapper<LongWritable, Text, Text, Text> {

        // Separate age by 10
        // Takes int age as input and outputs a discrete value
        private int ageFraming(int age){
            if (age >= 10 && age < 20)
                return 1;
            else if (age >= 20 && age < 30)
                return 2;
            else if (age >= 30 && age < 40)
                return 3;
            else if (age >= 40 && age < 50)
                return 4;
            else if (age >= 50 && age < 60)
                return 5;
            else if (age >= 60 && age <= 70)
                return 6;
            else return 0;
        }

        private HashMap<Integer, String[]> customer = new HashMap<>();
        // Read in Customer dataset and store into a hashmap
        public void setup(Context context) throws IOException, InterruptedException{
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0)
            {
                try{
                    FileSystem fs = FileSystem.get(context.getConfiguration());
                    Path path = new Path(cacheFiles[0].toString());
                    BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));
                    String line = null;
                    while((line = reader.readLine()) != null) {
                        String[] splitted = line.split(",");
                        Integer ID = Integer.parseInt(splitted[0]);
                        Integer age = ageFraming(Integer.parseInt(splitted[2]));
                        String gender = splitted[3];
                        String[] ls = {Integer.toString(age),gender};
                        customer.put(ID,ls);
                    }
//                    System.out.println(customer.get(27806)[0]);
//                    System.out.println(customer.get(27806)[1]);
                } catch(IOException ex) {
                    System.err.println("Exception in mapper setup: " + ex.getMessage());
                }
            }
        }

        private Text outputKey = new Text();
        private Text outputValue = new Text();

        // Mapper
        // output key = (agelabel,gender)
        // output value = transTotal
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            try{
//                System.out.print(customer.get("2"));
                String[] trans = value.toString().split(",");
                String[] ls = customer.get(Integer.parseInt(trans[1]));
                outputKey.set(ls[0] + ',' + ls[1]);
                outputValue.set(trans[2]);
                context.write(outputKey, outputValue);
//                System.out.println(trans[2]);
//                System.out.println(ls[0]);
            }
            catch (Exception ex) {
                System.err.println("Exception in broadcast join: " + ex.getMessage());
            }
        }
    }


    public static class MyReducer extends
            Reducer<Text, Text, Text, Text> {
        private HashMap<Integer, String> catFraming = new HashMap<Integer, String>() {{
            put(0, "Unknown");
            put(1, "[10, 20)");
            put(2, "[20, 30)");
            put(3, "[30, 40)");
            put(4, "[40, 50)");
            put(5, "[50, 60)");
            put(6, "[60, 70]");
        }};

        private Text outputKey = new Text();

        // Reducer
        // Takes in (key, value)
        // Output key agebucket (e.g. '[10,20)')
        // Output value: (gender, min, max, avg)
        @Override
        public void reduce(Text key, Iterable<Text> values,
                           Context context) throws IOException, InterruptedException {
            try{
                double min = 1001;
                double max = 0.0;
                double sum = 0;
                int cnt = 0;
                for (Text t : values)
                {
                    double val = Double.parseDouble(t.toString());
                    if(min > val) min = val;
                    if(max < val) max = val;
                    sum += val;
                    cnt ++;
                }
                double avg = sum/cnt;
                String[] k_v = key.toString().split(",");
                outputKey.set(catFraming.get(Integer.parseInt(k_v[0])));
                context.write(outputKey, new Text(k_v[1] + "," + min + "," + max + "," + avg ));
            }catch (Exception ex) {
                System.err.println("Exception in reducer: " + ex.getMessage());
            }
        }
    }

    // Main Function
    public static void main(String[] args) throws Exception {
//        testing code:
//        args = new String[3];
//        args[0] = "file:///E:/放桌面/DS503/project1/Customers.txt";
//        args[1] = "file:///E:/放桌面/DS503/project1/Transactions.txt";
//        args[2] = "file:///E:/放桌面/DS503/project1/output5.txt";
        BasicConfigurator.configure();
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // add small table to cache
        job.addCacheFile(new URI(args[0]));

        job.setJarByClass(Problem3_5.class);
        job.setMapperClass(TransMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(args[1]));
        //Output setup
        FileSystem fs = FileSystem.getLocal(conf);
        Path p = new Path(args[2]);
        if(fs.exists(p)){
            fs.delete(p, true);
        }
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
