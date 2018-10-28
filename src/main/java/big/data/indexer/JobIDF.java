package big.data.indexer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

public class JobIDF {
    public static class ReducerIDF extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        public void reduce(IntWritable key, final Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable ignored : values) {
                sum++;
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static class MapperIDF extends Mapper<IntWritable, MapWritable, IntWritable, IntWritable> {
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
            jobIDF.setMapperClass(MapperIDF.class);
            jobIDF.setReducerClass(ReducerIDF.class);
            jobIDF.setInputFormatClass(SequenceFileInputFormat.class);

            jobIDF.setMapOutputKeyClass(IntWritable.class);
            jobIDF.setMapOutputValueClass(IntWritable.class);

            jobIDF.setOutputKeyClass(IntWritable.class);
            jobIDF.setOutputValueClass(IntWritable.class);

            jobIDF.setOutputFormatClass(IntIntToMap.class);

            return jobIDF;
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
}
