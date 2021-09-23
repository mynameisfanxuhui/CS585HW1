// Problem 3-3
// author: xli14@wpi.edu

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Problem3_3 {

    // Mapper for Customer.txt
    public static class CustomerMapper
            extends Mapper<Object, Text, Text, Text> {

        private Text output_key = new Text();
        private Text output_value = new Text();

        // inputs each record from Customer.txt and output a <key, value> pair
        // outputs <customerID, ("customer", customerName, customerSalary)>
        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] str = value.toString().split(",");
            String ID = str[0];
            String name = str[1];
            String salary = str[5];
            output_key.set(ID);
            output_value.set(String.join(",", "customer", name, salary));
            context.write(output_key, output_value);
        }
    }

    // Mapper for Transactions.txt
    public static class TransactionMapper
            extends Mapper<Object, Text, Text, Text> {

        private Text outputKey = new Text();
        private Text outputValue = new Text();
        //inputs each record from Transactions.txt and output a <key, value> pair
        // Output Value: <custID, ("transaction", 1, totalTrans)>*/
        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] str = value.toString().split(",");
            String ID = str[1];
            String count = "1";
            String totalSum = str[2];
            String numItems = str[3];
            outputKey.set(ID);
            outputValue.set(String.join(",", "transaction", count, totalSum, numItems));
            context.write(outputKey, outputValue);
        }
    }

    // Combiner
    public static class Combiner
            extends Reducer<Text, Text, Text, Text>{

        private Text outputKey = new Text();
        private Text outputValue = new Text();

        // Receives <key, value> pairs from both kinds of Mappers
        // Input Value: <ID, ("customer", customerName, customerSalary)> or <ID, ("transaction", 1, totalTrans, numItems)>
        // Output Value: <ID, ("customer", customerName, customerSalary)> or <ID, ("transaction", NumOfTransactions, TotalSum, MinItems)
        @Override
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            int numTransactions = 0;
            float totalSum = 0;
            int minItem = 10;
            String customerName = "";
            String customerSalary = "";
            boolean flag = false;
            for (Text val : values) {
                String[] str = val.toString().split(",");
                if (str[0].equals("transaction")){
                    numTransactions += Integer.parseInt(str[1]);
                    totalSum += Double.parseDouble(str[2]);
                    minItem = Math.min(minItem,Integer.parseInt(str[3]));
                    flag = true;
                }else if (str[0].equals("customer")){
                    customerName = str[1];
                    customerSalary = str[2];
                }
            }
            if(flag){
                outputKey.set(key);
                outputValue.set(String.join(",", "transaction", String.valueOf(numTransactions), String.valueOf(totalSum), String.valueOf(minItem)));
            }else{
                outputKey.set(key);
                outputValue.set(String.join(",", "customer", customerName, customerSalary));
            }
            context.write(outputKey, outputValue);
        }
    }

    // Reducer of this Map-Reduce Job
    public static class FinalReducer
            extends Reducer<Text, Text, Text, NullWritable> {

        private Text outputKey = new Text();

        // receive <key, value> pairs from both kinds of Mappers and Combiners
        // Input Value: <ID, ("customer", customerName, customerName)> or <ID, ("transaction", count, subtotal, minItem)>
        // Output Value: <custID, customerName, numberOfTransactions, TransactionTotal
        @Override
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            float totalSum = 0;
            int numTransactions = 0;
            int minItem = 10;
            String customerName = "";
            String customerSalary = "";
            for (Text val : values) {
                String[] str = val.toString().split(",");

                if (str[0].equals("transaction")) {
                    numTransactions += Integer.parseInt(str[1]);
                    totalSum += Double.parseDouble(str[2]);
                    minItem = Math.min(minItem,Integer.parseInt(str[3]));
                }
                else if (str[0].equals("customer")){
                    customerName = str[1];
                    customerSalary = str[2];
                }

            }
            outputKey.set(String.join(",", key.toString(), customerName, customerSalary, String.valueOf(numTransactions), String.valueOf(totalSum), String.valueOf(minItem)));
            context.write(outputKey, NullWritable.get());
        }
    }


    // Main Function
    public static void main(String[] args) throws Exception {
//        testing code:
//        args = new String[3];
//        args[0] = "file:///E:/放桌面/DS503/project1/Customers.txt";
//        args[1] = "file:///E:/放桌面/DS503/project1/Transactions.txt";
//        args[2] = "file:///E:/放桌面/DS503/project1/output2.txt";
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Problem 3.3");
        job.setJarByClass(Problem3_1.class);
        job.setCombinerClass(Combiner.class);
        job.setReducerClass(FinalReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        MultipleInputs.addInputPath(job,new Path(args[0]),TextInputFormat.class,CustomerMapper.class);
        MultipleInputs.addInputPath(job,new Path(args[1]),TextInputFormat.class,TransactionMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
