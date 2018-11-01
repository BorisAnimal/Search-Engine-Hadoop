package big.data.searcher;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Map;

import static big.data.Tools.IdfMultiTool.parseQueryStringToMap;


public class SearchJob {

    public static class SearchMapper extends Mapper<IntWritable, MapWritable, IntWritable, DoubleWritable> {
        private Map<Integer, Double> query;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            try {
                Configuration conf = context.getConfiguration();
                if (conf.get("query") != null) {
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
        protected void map(IntWritable key, MapWritable value, Context context) throws IOException, InterruptedException {
            double sum = 0.0;
            for (Integer k : query.keySet()) {
                IntWritable kek = new IntWritable(k);
                if (value.containsKey(kek)) {
                    sum += ((DoubleWritable) value.get(kek)).get();
                }
            }
            context.write(key, new DoubleWritable(sum));
        }
    }


    public static class SearchReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
        @Override
        protected void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            super.reduce(key, values, context);
        }
    }


    public static Job getJob(Configuration config) throws Exception {
        try {
            Job job = Job.getInstance(config, "tfidf_engine");
            job.setJarByClass(SearchEngine.class);
            job.setMapperClass(SearchMapper.class);
            job.setReducerClass(SearchReducer.class);
            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(DoubleWritable.class);

            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(DoubleWritable.class);

            return job;
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
}
