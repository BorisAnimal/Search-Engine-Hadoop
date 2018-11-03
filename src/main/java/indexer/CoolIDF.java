package indexer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

// Cool, because it finally works, not like JobIDF do
public class CoolIDF {

    public static class CoolReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        private final IntWritable ONE = new IntWritable(1);

        public void reduce(IntWritable key, final Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable ignored : values) {
                sum++;
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static class CoolMapper extends Mapper<IntWritable, MapWritable, IntWritable, IntWritable> {
        private final IntWritable ONE = new IntWritable(1);

        @Override
        public void map(IntWritable key, MapWritable value, Context context) throws IOException, InterruptedException {
            for (Writable i : value.keySet()) {
                context.write((IntWritable) i, ONE);
            }
        }
    }


    static Job getJob(Configuration conf) throws Exception {
        try {
            Job jobIDF = Job.getInstance(conf, "idf_engine");
            jobIDF.setJarByClass(IndexEngine.class);
            jobIDF.setMapperClass(CoolMapper.class);
            jobIDF.setReducerClass(CoolReducer.class);
            jobIDF.setInputFormatClass(SequenceFileInputFormat.class);

            jobIDF.setMapOutputKeyClass(IntWritable.class);
            jobIDF.setMapOutputValueClass(IntWritable.class);

            jobIDF.setOutputKeyClass(IntWritable.class);
            jobIDF.setOutputValueClass(Text.class);

            jobIDF.setOutputFormatClass(TextOutputFormat.class);

            return jobIDF;
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }


}
