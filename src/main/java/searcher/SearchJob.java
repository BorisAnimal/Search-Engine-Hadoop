package searcher;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Map;

import static Tools.IdfMultiTool.parseCycle;
import static Tools.IdfMultiTool.parseQueryStringToMap;


public class SearchJob {

    public static class SearchMapper extends Mapper<LongWritable, Text, LongWritable, DoubleWritable> {
        private Map<Integer, Double> query = null;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            try {
                Configuration conf = context.getConfiguration();
                if (conf.get("query") != null) {
                    if (query == null)
                        query = parseQueryStringToMap(conf.get("query"));
                } else {
                    throw new IOException("No query in configurations!!");
                }
            } catch (Exception ex) {
                ex.printStackTrace();
                throw (IOException) ex;
            }
        }


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            double sum = 0.0;
            Map cycle = parseCycle(value.toString());
//            System.out.println("\n");
//            System.out.println(value);
//            System.out.println("\n");
            if (query == null)
                System.out.println("null query");
            for (Integer k : query.keySet()) {
                IntWritable kek = new IntWritable(k);
                if (cycle != null && cycle.containsKey(kek)) {
                    DoubleWritable d = (DoubleWritable) cycle.get(kek);
                    if (d != null)
                        sum += d.get();
                }
            }
            if (sum > 0.0) {
                context.write(key, new DoubleWritable(sum));
                System.out.println(key + ": " + sum);
            }
        }

    }


    public static class SearchReducer extends Reducer<LongWritable, DoubleWritable, LongWritable, DoubleWritable> {
        @Override
        protected void reduce(LongWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            System.out.println(key + ":" + values);
            super.reduce(key, values, context);
        }
    }


    public static Job getJob(Configuration config) throws Exception {
        try {
            Job job = Job.getInstance(config, "Searching engine");
            job.setJarByClass(SearchEngine.class);
            job.setMapperClass(SearchMapper.class);
            job.setReducerClass(SearchReducer.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(DoubleWritable.class);

            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(DoubleWritable.class);

            return job;
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
}
