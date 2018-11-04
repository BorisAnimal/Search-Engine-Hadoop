package indexer;

import Tools.IdfMultiTool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Map;


public class JobTFIDF {
    public static class MapperTFIDF extends Mapper<IntWritable, MapWritable, IntWritable, MapWritable> {
        private Map<Integer, Integer> idf;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            try {
                Configuration conf = context.getConfiguration();
                if (conf.get("idf") != null) {
                    idf = IdfMultiTool.parseStringToMap(conf.get("idf"));
                } else {
                    throw new IOException("No idf cache file!!");
                }
            } catch (Exception ex) {
                ex.printStackTrace();
                throw (IOException) ex;
            }
        }

        @Override
        public void map(IntWritable key, MapWritable value, Context context) throws IOException, InterruptedException {
            for (Writable k : value.keySet()) {
                double tfidf = (((IntWritable) value.get(k)).get() * 1.0) / idf.get(((IntWritable) k).get());
                value.put(k, new DoubleWritable(tfidf));
            }
            context.write(key, value);
        }
    }


    public static class IndexReducer extends Reducer<IntWritable, MapWritable, IntWritable, MapWritable> {

        private boolean hasVoidFields(MapWritable map) {
            for (Map.Entry<Writable, Writable> k: map.entrySet()) {
                if (k.getKey() == null || k.getValue() == null)
                    return true;
            }
            return false;
        }

        public void reduce(IntWritable key, final Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
            for (MapWritable map : values) {
                try {
                    if (key != null && map != null && !map.isEmpty() && !hasVoidFields(map))
                        context.write(key, map);
                    else
                        System.out.println("!!!! CATCHERDD!!!!! 111");
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
//                System.out.println(map.entrySet() + "");
            }
        }
    }

    static Job getJob(Configuration conf) throws Exception {
        try {
            Job jobTFIDF = Job.getInstance(conf, "tfidf_engine");
            jobTFIDF.setJarByClass(IndexEngine.class);
            jobTFIDF.setMapperClass(MapperTFIDF.class);
            jobTFIDF.setReducerClass(IndexReducer.class);
            jobTFIDF.setInputFormatClass(SequenceFileInputFormat.class);
            jobTFIDF.setOutputFormatClass(SequenceFileOutputFormat.class);
            jobTFIDF.setOutputKeyClass(IntWritable.class);
            jobTFIDF.setOutputValueClass(MapWritable.class);

            jobTFIDF.setMapOutputKeyClass(IntWritable.class);
            jobTFIDF.setMapOutputValueClass(MapWritable.class);

            return jobTFIDF;
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
}
