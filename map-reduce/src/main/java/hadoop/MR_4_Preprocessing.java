package hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class MR_4_Preprocessing {

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

    public static class UpperCaseMapper extends Mapper<Text, LongWritable, Text, LongWritable> {

        @Override
        public void map(Text key, LongWritable value, Context context) throws IOException, InterruptedException {
            context.write(new Text(key.toString().toUpperCase()), value);
        }

    }

    public static class InvalidCharactersMapper extends Mapper<Text, LongWritable, Text, LongWritable> {

        @Override
        public void map(Text key, LongWritable value, Context context) throws IOException, InterruptedException {
            context.write(new Text(key.toString().replaceAll("[.,]", "")), value);
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
        Job job = Job.getInstance(configuration, "Word Count 1.3 - Pre/Postprocessing");

        job.setJarByClass(MR_4_Preprocessing.class);

        ChainMapper.addMapper(job, TokenizerMapper.class, Object.class, Text.class, Text.class, LongWritable.class, configuration);
        ChainMapper.addMapper(job, UpperCaseMapper.class, Text.class, LongWritable.class, Text.class, LongWritable.class, configuration);
        ChainMapper.addMapper(job, InvalidCharactersMapper.class, Text.class, LongWritable.class, Text.class, LongWritable.class, configuration);

        ChainReducer.setReducer(job, LongSumReducer.class, Text.class, LongWritable.class, Text.class, LongWritable.class, configuration);
        ChainReducer.addMapper(job, KeyValueSwappingMapper.class, Text.class, LongWritable.class, LongWritable.class, Text.class, configuration);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}