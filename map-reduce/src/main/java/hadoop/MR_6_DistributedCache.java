package hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

public class MR_6_DistributedCache {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, LongWritable> {

        private final static LongWritable ONE = new LongWritable(1);
        private final Text word = new Text();
        private final Set<String> patternsToSkip = new HashSet<>();

        @Override
        public void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();

            URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
            for (URI patternsURI : patternsURIs) {
                Path patternsPath = new Path(patternsURI.getPath());
                String patternsFileName = patternsPath.getName();
                parseSkipFile(patternsFileName);
            }
        }

        private void parseSkipFile(String fileName) {
            try {
                BufferedReader fis = new BufferedReader(new FileReader(fileName));
                String pattern;
                while ((pattern = fis.readLine()) != null) {
                    patternsToSkip.add(pattern);
                }
            } catch (IOException ioe) {
                System.err.println("Caught exception while parsing the cached file '" + StringUtils.stringifyException(ioe));
            }
        }


        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String token = itr.nextToken();
                if (!patternsToSkip.contains(token)) {
                    word.set(token);
                    context.write(word, ONE);
                }
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
        Job job = Job.getInstance(configuration, "Word Count 1.5 - Distributed Cache");

        job.setJarByClass(MR_6_DistributedCache.class);

        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(LongSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.addCacheFile(new Path(args[2]).toUri());

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}