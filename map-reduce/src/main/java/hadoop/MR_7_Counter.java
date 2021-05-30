package hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class MR_7_Counter {

    enum Punctuation {
        ANY,
        DOT,
        COMMA
    }

    public static class TokenizerMapper extends Mapper<Object, Text, Text, LongWritable> {

        private final static LongWritable ONE = new LongWritable(1);
        private final Text word = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String token = itr.nextToken();
                if (token.endsWith(",")) {
                    context.getCounter(Punctuation.ANY).increment(1);
                    context.getCounter(Punctuation.COMMA).increment(1);
                }
                else if (token.endsWith(".")) {
                    context.getCounter(Punctuation.ANY).increment(1);
                    context.getCounter(Punctuation.DOT).increment(1);
                }

                word.set(token);
                context.write(word, ONE);
            }
        }
    }

    public static class LongSumReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

        private final LongWritable result = new LongWritable();

        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "Word Count 1.6 - Counters");

        job.setJarByClass(MR_7_Counter.class);

        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(LongSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

        for (CounterGroup group : job.getCounters()) {
            System.out.println("* Counter Group: " + group.getDisplayName() + " (" + group.getName() + ")");
            System.out.println("  number of counters in this group: " + group.size());
            for (Counter counter : group) {
                System.out.println("  - " + counter.getDisplayName() + ": " + counter.getName() + ": "+counter.getValue());
            }
        }

        System.exit(0);
    }

}