import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;

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


public class Problem2_3 {

    public static class DetectionMapper
            extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            try{
                String r = context.getConfiguration().get("radius");

                PostionCoordinate pointPos = new PostionCoordinate(value.toString());
                //Which segments it is in, we do not want to count duplicate
                PostionCoordinate segmentPos = pointPos.getSegment(Integer.parseInt(r));
                context.write(new Text(segmentPos.toStrOutput()), new Text("M" + "," + pointPos.toStrOutput()));
                // this point may be which segments' neighbour
                //down
                context.write(new Text((segmentPos.x-1) + ","+ segmentPos.y), new Text("N" + "," + pointPos.toStrOutput()));
                //up
                context.write(new Text((segmentPos.x+1) + ","+ segmentPos.y), new Text("N" + "," + pointPos.toStrOutput()));
                //left
                context.write(new Text(segmentPos.x + "," + (segmentPos.y-1)), new Text("N" + "," + pointPos.toStrOutput()));
                //right
                context.write(new Text(segmentPos.x + ","+ (segmentPos.y + 1)), new Text("N" + "," + pointPos.toStrOutput()));
                //leftUp
                context.write(new Text((segmentPos.x+1) + ","+ (segmentPos.y-1)), new Text("N" + "," + pointPos.toStrOutput()));
                //leftDown
                context.write(new Text((segmentPos.x-1) + ","+ (segmentPos.y-1)), new Text("N" + "," + pointPos.toStrOutput()));
                //rightUp
                context.write(new Text((segmentPos.x+1) + ","+ (segmentPos.y+1)), new Text("N" + "," + pointPos.toStrOutput()));
                //rightDown
                context.write(new Text((segmentPos.x-1) + ","+ (segmentPos.y+1)), new Text("N" + "," + pointPos.toStrOutput()));

            }
            catch (Exception e)
            {
                System.out.println("map having problem" + e.toString());
            }
        }
    }




    // Reducer of this Map-Reduce Job
    public static class DetectionReducer
            extends Reducer<Text, Text, Text, NullWritable> {
        private Text outputKey = new Text();
        @Override
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            try{

                int r = Integer.parseInt(context.getConfiguration().get("radius"));
                int k = Integer.parseInt(context.getConfiguration().get("kNeighbor"));
                List<PostionCoordinate> inSegment = new ArrayList<PostionCoordinate>();
                List<PostionCoordinate> possibleNeighbour = new ArrayList<PostionCoordinate>();
                for (Text val : values) {

                    String[] str = val.toString().split(",");
                    PostionCoordinate currentPos = new PostionCoordinate(Integer.parseInt(str[1]), Integer.parseInt(str[2]));
                    if (Objects.equals(str[0], "M"))
                    {
                        inSegment.add(currentPos);
                    }
                    // all Points are possible neighbour
                    possibleNeighbour.add(currentPos);
                }
                System.out.println("key is" + key.toString());
                for (PostionCoordinate pointPos: inSegment)
                {
                    System.out.println("pos is" + pointPos.x + pointPos.y);
                    //will add itself, so reduce one at the beginning
                    int neighbourNum = -1;
                    for (PostionCoordinate neighbour: possibleNeighbour)
                    {
                        //whithin, add one neighbour
                        if (pointPos.returnDis(neighbour) <= (r * r))
                        {
                            neighbourNum += 1;
                            // not an outliner, next
                            System.out.println("neighbour is"+ neighbourNum);
                            if (neighbourNum >= k) break;
                        }
                    }
                    if (neighbourNum < k) context.write(new Text(pointPos.toStrOutput() + "\n"), NullWritable.get());

                }
            }catch (Exception e)
            {
                System.out.println("reducer having problem");
            }
        }

    }



    public static void main(String[] args) throws Exception {
        //parameters
        String inputPath = "inputData.txt";
        String outputPath = "Problem2_3";
        String k = "3";
        String r = "5";

        Configuration conf = new Configuration();
        conf.set("radius", r);
        conf.set("kNeighbor", k);

        Job job = Job.getInstance(conf, "2_3");
        job.setJarByClass(Problem2_3.class);

        //job.getConfiguration().set("mapreduce.output.basename", "problem2_3");


        job.setMapperClass(DetectionMapper.class);
        job.setReducerClass(DetectionReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.setInputPaths(job, inputPath);
        FileSystem fs = FileSystem.getLocal(conf);
        Path p = new Path(outputPath);
        if(fs.exists(p)){
            fs.delete(p, true);
        }
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}