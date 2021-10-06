import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;

import javafx.geometry.Pos;
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
public class Problem2_4 {

    public static class KMeansMapper
            extends Mapper<Object, Text, Text, Text> {

        List<PostionCoordinate> centers = new ArrayList<>();
        private Text output_key = new Text();
        private Text output_value = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException{
            try{
                this.centers = new ArrayList<>();
                System.out.println("this is iteration" + context.getConfiguration().get("iteration"));
                URI[] cachedFiles = context.getCacheFiles();
                String thisCache = context.getConfiguration().get("centerPath");
                System.out.println("cache is" + thisCache);
                for (URI cacheFile: cachedFiles)
                {
                    System.out.println("finename" + cacheFile.toString());
                    if (cacheFile.toString().equals(thisCache))
                    {
                        BufferedReader br = new BufferedReader(new FileReader(cacheFile.toString()));
                        String line;
                        System.out.println("first break");
                        line = br.readLine();
                        while (line!= null)
                        {
                            if (line == "") break;
                            System.out.println("center pos is" + line);
                            PostionCoordinate centerPos = new PostionCoordinate(line);
                            System.out.println("add center" + centerPos.toStrOutput());
                            this.centers.add(centerPos);
                            line = br.readLine();
                        }
                        System.out.println("second break");

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
                PostionCoordinate currentPos = new PostionCoordinate(value.toString());
                // max number is 10000 * 10000
                int minDis = 100000000;
                PostionCoordinate minCenter = new PostionCoordinate(currentPos.toStrOutput());


                for (PostionCoordinate oCenter : this.centers)
                {
                    int iDis = currentPos.returnDis(oCenter);
                    System.out.println("dis is "+iDis);
                    if (iDis < minDis)
                    {
                        minDis = iDis;
                        minCenter = oCenter;
                    }
                }
                System.out.println("minCenter is null" + minCenter.toStrOutput());
                //only one reducer, all the key is same
                output_key.set("1");
                output_value.set(minCenter.toStrOutput()+"," + currentPos.toStrOutput() + "," + "1");
                context.write(output_key, output_value);
            }
            catch (Exception e)
            {
                System.out.println("map having problem" + e.toString());
            }
        }
    }

    public static class KMeansCombiner
            extends Reducer<Text, Text, Text, Text> {
        private Text outputValue = new Text();
        @Override
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            try {
                Map<String, String> posDict = new HashMap<>();
                for (Text val : values) {
                    System.out.println("Val is" + val);
                    String[] str = val.toString().split(",");

                    String centerPos = str[0] + "," + str[1];
                    if (!posDict.containsKey(centerPos))
                    {
                        posDict.put(centerPos, str[2] + "," + str[3] + "," + str[4]);
                    }
                    else
                    {
                        String[] infoStr = posDict.get(centerPos).split(",");
                        long xSum = Integer.parseInt(infoStr[0]) + Integer.parseInt(str[2]);
                        long ySum = Integer.parseInt(infoStr[1]) + Integer.parseInt(str[3]);
                        long totalNum = Integer.parseInt(infoStr[2]) + Integer.parseInt(str[4]);
                        posDict.put(centerPos, xSum + "," + ySum + "," + totalNum);
                    }
                }
                String outputStr = "";
                for (HashMap.Entry<String, String> entry : posDict.entrySet())
                {
                    String centerPos = entry.getKey();
                    String sumInfo = entry.getValue();
                    outputStr += (centerPos + "," + sumInfo);
                }
                outputValue = new Text(outputStr);
                context.write(key, outputValue);

            }catch (Exception e)
            {
                System.out.println("combiner having problem" + e.toString());
            }
        }
    }


    // Reducer of this Map-Reduce Job
    public static class KMeansReducer
            extends Reducer<Text, Text, Text, NullWritable> {
        public static enum Counter {
            CHANGEDNUMBER
        }
        private Text outputKey = new Text();
        @Override
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            try{
                context.getConfiguration().set("ww", "1");
                System.out.println("counter is" + context.getCounter(Counter.CHANGEDNUMBER).getValue());

                Map<String, String> posDict = new HashMap<>();
                for (Text val : values) {
                    String[] str = val.toString().split(",");
                    for (int i = 0; i < str.length; i += 5)
                    {
                        String centerPos = str[i] + "," + str[i + 1];
                        if (!posDict.containsKey(centerPos))
                        {
                            posDict.put(centerPos, str[i + 2] + "," + str[i + 3] + "," + str[i + 4]);
                        }
                        else
                        {
                            String[] infoStr = posDict.get(centerPos).split(",");
                            long xSum = Integer.parseInt(infoStr[0]) + Integer.parseInt(str[i + 2]);
                            long ySum = Integer.parseInt(infoStr[1]) + Integer.parseInt(str[i + 3]);
                            long totalNum = Integer.parseInt(infoStr[2]) + Integer.parseInt(str[i + 4]);
                            posDict.put(centerPos, xSum + "," + ySum + "," + totalNum);
                        }
                    }

                }
                String outputStr = "";
                for (HashMap.Entry<String, String> entry : posDict.entrySet())
                {
                    String centerPos = entry.getKey();
                    String[] sumInfo = entry.getValue().split(",");
                    String newCenter = "";
                    long xSum = Integer.parseInt(sumInfo[0]);
                    long ySum = Integer.parseInt(sumInfo[1]);
                    long totalNum = Integer.parseInt(sumInfo[2]);
                    newCenter = xSum / totalNum + "," + ySum / totalNum;
                    if (!centerPos.equals(newCenter))
                    {
                        context.getCounter(Counter.CHANGEDNUMBER).increment(1);
                    }
                    outputStr += (newCenter + "\n");
                }
                System.out.println("after counter is" + context.getCounter(Counter.CHANGEDNUMBER).getValue());
                outputKey = new Text(outputStr);
                context.write(outputKey, NullWritable.get());
            }catch (Exception e)
            {
                System.out.println("reducer having problem" + e.toString());
            }
        }
    }



    public static void main(String[] args) throws Exception {
        //max iteration is 10
        int maxIteration = 10;
        int k = 10;
        int iteration = 0;
        //parameters
        String inputPath = "inputData.txt";
        String outputPath = "/Users/torresfan/Desktop/Problem2_4";
        String centroidsPath = "/Users/torresfan/Desktop/centroids.txt";
        String outputFileName = "/part-r-00000";
        GenerateCenter oGenerate = new GenerateCenter();
        oGenerate.creatCenter(centroidsPath, k, 1, 10000);

        Configuration conf = new Configuration();
        conf.set("iteration", String.valueOf(iteration));
        conf.set("centerPath", centroidsPath);
        Job job = Job.getInstance(conf, "Problem2_4" + iteration);
        //set reducer number to 1
        job.setNumReduceTasks(1);
        Path in = new Path(inputPath);
        Path center = new Path(centroidsPath);
        Path out = new Path(outputPath + iteration);


        job.setMapperClass(KMeansMapper.class);
        job.setCombinerClass(KMeansCombiner.class);
        job.setReducerClass(KMeansReducer.class);
        job.setJarByClass(Problem2_4.class);
        FileInputFormat.addInputPath(job, in);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(out)) {
            fs.delete(out, true);
        }

        FileOutputFormat.setOutputPath(job, out);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.addCacheFile(center.toUri());
        job.waitForCompletion(true);
        System.out.println("this is config test" + job.getConfiguration().get("ww"));
        //job.getConfiguration().set(KMeansMapper.CENTER_PATH, center.getName());

        long counter = job.getCounters().findCounter(KMeansReducer.Counter.CHANGEDNUMBER).getValue();
        iteration++;
        while (counter > 0 && iteration < maxIteration) {
            conf = new Configuration();
            conf.set("iteration", String.valueOf(iteration));
            conf.set("centerPath", outputPath + (iteration-1) + outputFileName);
            job = Job.getInstance(conf, "Problem2_4" + iteration);
            //set reducer number to 1
            job.setNumReduceTasks(1);
            out = new Path(outputPath + iteration);
            center = new Path(outputPath + (iteration-1) + outputFileName);

            job.setMapperClass(KMeansMapper.class);
            job.setCombinerClass(KMeansCombiner.class);
            job.setReducerClass(KMeansReducer.class);
            job.setJarByClass(Problem2_4.class);
            FileInputFormat.addInputPath(job, in);
            fs = FileSystem.get(conf);
            if (fs.exists(out)) {
                fs.delete(out, true);
            }
            FileOutputFormat.setOutputPath(job, out);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);
            job.addCacheFile(center.toUri());
            job.waitForCompletion(true);
            iteration ++;
            counter = job.getCounters().findCounter(KMeansReducer.Counter.CHANGEDNUMBER).getValue();
            System.out.println("counter2 is" + job.getCounters().findCounter(KMeansReducer.Counter.CHANGEDNUMBER).getValue());
        }

    }
}