package big.data.indexer;

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
import org.json.JSONObject;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class JobTFIDF {
    public static class MapperTFIDF extends Mapper<IntWritable, MapWritable, IntWritable, MapWritable> {
        private Map<Integer, Integer> idf;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            try {
                Configuration conf = context.getConfiguration();


                if (conf.get("idf") != null ) {
//                    idf = new MapWritable();
//                    FileSystem fs = FileSystem.get(context.getConfiguration());
//                    Path path = new Path(context.getCacheFiles()[0].toString() + "/part-r-00000");
//                    DataInputStream reader = new DataInputStream(fs.open(path));
//                    idf.readFields(reader);
                    String param = conf.get("idf");
                    JSONObject tmp = new JSONObject(param);
                    Map tmpMap = tmp.toMap();
                    idf = new HashMap<Integer, Integer>();
                    for (Object i : tmpMap.keySet()) {
                        idf.put(new Integer(i + ""), new Integer(tmpMap.get(i) + ""));
                    }
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
                value.put(k, new DoubleWritable(((IntWritable) value.get(k)).get() / idf.get(new Integer(((IntWritable) k).get()))));
            }
            context.write(key, value);
        }
    }


    public static class IndexReducer extends Reducer<IntWritable, MapWritable, IntWritable, MapWritable> {


        public void reduce(IntWritable key, final Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
            System.out.println("REDUCING");
            for (MapWritable map : values) {
                try {
                    System.out.println("Class: " + key.getClass());
                    context.write(key, map);
                }
                catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
            System.out.println("hue_REDUCING");
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
