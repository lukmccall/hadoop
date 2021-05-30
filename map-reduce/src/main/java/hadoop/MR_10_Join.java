package hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class MR_10_Join {

    public static class OneMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String token = itr.nextToken();
                String[] parts = token.split(",");
                System.out.println("One|" + String.join(",", parts[1], parts[2], parts[3], parts[4]));
                context.write(new Text(parts[0]),  new Text("One|" + String.join(",", parts[1], parts[2], parts[3], parts[4])));
            }
        }

    }

    public static class TwoMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String token = itr.nextToken();
                String[] parts = token.split(",");
                System.out.println("Two|" + String.join(",", parts[0], parts[1], parts[3]));
                context.write(new Text(parts[2]),  new Text("Two|" + String.join(",", parts[0], parts[1], parts[3])));
            }
        }

    }

    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String beggining = "";
            List<String> list = new ArrayList<>();
            for (Text val : values) {
                String[] parts = val.toString().split("\\|");
                if (parts[0].equals("One")) {
                    beggining = parts[1];
                }
                else if (parts[0].equals("Two")) {
                    list.add(parts[1]);
                }
            }

            for (String element: list) {
                context.write(new Text(beggining), new Text(element));
            }
        }

    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "Word Count 1.9 - Join");

        job.setJarByClass(MR_10_Join.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, OneMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TwoMapper.class);
        job.setReducerClass(JoinReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}