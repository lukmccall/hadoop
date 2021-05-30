package hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

public class MR_8_MapToClass {

    public static class One implements WritableComparable<One> {

        private int id;
        private String firstName;
        private String lastName;

        public One(int id, String firstName, String lastName) {
            this.id = id;
            this.firstName = firstName;
            this.lastName = lastName;
        }

        public One() {

        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public String getFirstName() {
            return firstName;
        }

        public void setFirstName(String firstName) {
            this.firstName = firstName;
        }

        public String getLastName() {
            return lastName;
        }

        public void setLastName(String lastName) {
            this.lastName = lastName;
        }

        @Override
        public void readFields(DataInput dataIp) throws IOException {
            id = dataIp.readInt();
            firstName = dataIp.readUTF();
            lastName = dataIp.readUTF();
        }

        @Override
        public void write(DataOutput dataOp) throws IOException {
            dataOp.writeInt(id);
            dataOp.writeUTF(firstName);
            dataOp.writeUTF(lastName);
        }

        @Override
        public int compareTo(One arg0) {
            return Integer.compare(id, arg0.id);
        }

        @Override
        public String toString() {
            return id + "," + firstName + "," + lastName;
        }

    }

    public static class OneMapper extends Mapper<Object, Text, One, LongWritable> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String token = itr.nextToken();
                String[] split = token.split(",");
                context.write(new One(Integer.parseInt(split[0]), split[1], split[2]), new LongWritable(1));
            }
        }

    }

    public static class OneReducer extends Reducer<One, LongWritable, One, LongWritable> {

        private final LongWritable result = new LongWritable();

        @Override
        public void reduce(One key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
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
        Job job = Job.getInstance(configuration, "Word Count 1.7 - Mapping to Class");

        job.setJarByClass(MR_8_MapToClass.class);

        job.setMapperClass(OneMapper.class);
        job.setReducerClass(OneReducer.class);

        job.setOutputKeyClass(One.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}