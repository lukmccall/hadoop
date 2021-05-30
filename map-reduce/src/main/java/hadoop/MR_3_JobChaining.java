package hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class MR_3_JobChaining {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, LongWritable> {

        private final static LongWritable ONE = new LongWritable(1);
        private final Text word = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
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

    public static class KeyValueSwappingMapper extends Mapper<Text, LongWritable, LongWritable, Text> {

        @Override
        public void map(Text key, LongWritable value, Context context) throws IOException, InterruptedException {
            context.write(value, key);
        }

    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "Word Count 1.2 - Counting");

        job.setJarByClass(MR_3_JobChaining.class);

        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(LongSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

        Job job2 = Job.getInstance(configuration, "Word Count 1.2 - Sorting");

        job2.setJarByClass(MR_3_JobChaining.class);

        job2.setInputFormatClass(SequenceFileInputFormat.class);

        job2.setMapperClass(KeyValueSwappingMapper.class);
        job2.setSortComparatorClass(LongWritable.DecreasingComparator.class);
        job2.setReducerClass(Reducer.class);

        job2.setOutputKeyClass(LongWritable.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }

}