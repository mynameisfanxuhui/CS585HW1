import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Problem3_2 {

        public static class CustomerMapper
                extends Mapper<Object, Text, Text, Text> {
            private Text output_key = new Text();
            private Text output_value = new Text();

            public void map(Object key, Text value, Context context
            ) throws IOException, InterruptedException {
                String[] str = value.toString().split(",");
                String ID = str[0];
                String name = str[1];
                output_key.set(ID);
                String output = String.join(",", "c", name);
                output_value.set(output);
                context.write(output_key, output_value);
            }
        }


    public static class TransactionMapper
            extends Mapper<Object, Text, Text, Text> {
        private Text output_key = new Text();
        private Text output_value = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] str = value.toString().split(",");
            String ID = str[1];
            String transTotal = str[2];
            output_key.set(ID);
            output_value.set(String.join(",", "t","1", transTotal));
            context.write(output_key, output_value);
        }
    }




    // Reducer of this Map-Reduce Job
    public static class SumCombiner
            extends Reducer<Text, Text, Text, Text> {

        private Text outputKey = new Text();
        private Text outputValue = new Text();
        @Override
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            outputKey.set(key);
            float totalSum = 0;
            int numTransactions = 0;
            String customerName = "";
            int isCustomer = 0;
            for (Text val : values) {
                String[] str = val.toString().split(",");
                if (str[0].equals("t")) {

                    numTransactions += Integer.parseInt(str[1]);
                    totalSum += Double.parseDouble(str[2]);
                }
                else{
                    isCustomer = 1;
                    customerName = str[1];
                }
            }
            String outputStr = "";
            if (isCustomer == 1)
            {
                outputStr = String.join(",", "c", customerName);
            }
            else
            {
                outputStr = String.join(",", "t", String.valueOf(numTransactions), String.valueOf(totalSum));
            }
            outputValue.set(outputStr);
            context.write(outputKey, outputValue);
        }
    }


    // Reducer of this Map-Reduce Job
    public static class SumReducer
            extends Reducer<Text, Text, Text, NullWritable> {
        private Text outputKey = new Text();
        @Override
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            float totalSum = 0;
            int numTransactions = 0;
            int hasCustomer = 0;
            String customerName = "";
            for (Text val : values) {
                String[] str = val.toString().split(",");
                if (str[0].equals("t")) {

                    numTransactions += Integer.parseInt(str[1]);
                    totalSum += Double.parseDouble(str[2]);
                }
                else{
                    customerName = str[1];
                    hasCustomer = 1;
                }
            }
            String outputStr = "";
            if (hasCustomer == 1)
            {
                outputStr = String.join(",", key.toString(), customerName, String.valueOf(numTransactions), String.valueOf(totalSum));
            }
            else
            {
                // no customer, should not output
                outputStr = "";
            }
            outputKey.set(outputStr);
            context.write(outputKey, NullWritable.get());
        }
    }



        public static void main(String[] args) throws Exception {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "3_2");
            job.setJarByClass(Problem3_2.class);
            job.setCombinerClass(SumCombiner.class);
            job.setReducerClass(SumReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            MultipleInputs.addInputPath(job,new Path(args[0]), TextInputFormat.class,CustomerMapper.class);
            MultipleInputs.addInputPath(job,new Path(args[1]),TextInputFormat.class,TransactionMapper.class);
            FileSystem fs = FileSystem.getLocal(conf);
            Path p = new Path(args[2]);
            if(fs.exists(p)){
                fs.delete(p, true);
            }
            FileOutputFormat.setOutputPath(job, new Path(args[2]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
    }


