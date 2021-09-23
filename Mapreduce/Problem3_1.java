import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Problem3_1 {
    public static class CustomerMapper
            extends Mapper<Object, Text, Text, Text> {

        private Text output_key = new Text();
        private Text output_value = new Text();

        /**
         * This method inputs each record from Customer.txt and output a <key, value> pair
         * Output Value: <ID, (other values except ID)>
         */
        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] str = value.toString().split(",");
            int age = Integer.valueOf(str[2]).intValue();
            if(age<=50&&age>=20){
                String ID = str[0];
                output_key.set(ID);
                output_value.set(value.toString().replace(ID, ""));
                context.write(output_key, output_value);
            }

        }
    }


    public static void main(String[] args) throws Exception {
//        args = new String[3];
//        args[0] = "file:///E:/放桌面/DS503/project1/Customers.txt";
//        args[1] = "file:///E:/放桌面/DS503/project1/Customers.txt";
//        args[2] = "file:///E:/放桌面/DS503/project1/3-1.txt";

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Problem 3.1");
        job.setJarByClass(Problem3_1.class);
//        job.setCombinerClass(ResultCombiner.class);
//        job.setReducerClass(ProbReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        MultipleInputs.addInputPath(job,new Path(args[0]),TextInputFormat.class,CustomerMapper.class);
        MultipleInputs.addInputPath(job,new Path(args[1]),TextInputFormat.class,CustomerMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
