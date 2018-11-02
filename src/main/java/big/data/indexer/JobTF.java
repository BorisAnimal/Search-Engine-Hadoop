package big.data.indexer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import static big.data.Tools.IdfMultiTool.getSkipPattern;
import static big.data.Tools.IdfMultiTool.isCaseSensitive;

public class JobTF {
    public static class MapperTF extends Mapper<Object, Text, IntWritable, IntWritable> {
        private IntWritable whash = new IntWritable();
        private Configuration conf;
        private BufferedReader fis;

        @Override
        public void setup(Context context) throws IOException {
            conf = context.getConfiguration();
//            caseSensitive = conf.getBoolean("wordcount.case.sensitive", true);
        }

        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = (isCaseSensitive()) ? value.toString() : value.toString().toLowerCase();
            String[] tuple = line.split("\\n");
            try {
                // For each document (JSON)
                for (String str : tuple) {
                    JSONObject obj = new JSONObject(str);
                    IntWritable d_id = new IntWritable(obj.getInt("id"));
                    String text = obj.getString("text").replaceAll(getSkipPattern(), "");
                    StringTokenizer itr = new StringTokenizer(text);
                    while (itr.hasMoreTokens()) {
                        whash.set(itr.nextToken().hashCode());
                        context.write(d_id, whash);
                    }
                }
            } catch (JSONException e) {
                e.printStackTrace();
            }
        }
    }

    public static class ReducerTF extends Reducer<IntWritable, IntWritable, IntWritable, MapWritable> {
        private final IntWritable ONE = new IntWritable(1);

        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            MapWritable counter = new MapWritable();
            for (IntWritable val : values) {
                if (counter.containsKey(val)) {
                    counter.put(new IntWritable(val.get()), new IntWritable(((IntWritable) counter.get(val)).get() + 1));
                } else {
                    counter.put(new IntWritable(val.get()), ONE);
                }
            }
            context.write(key, counter);
        }
    }

    static Job getJob(Configuration conf) throws Exception {
        try {
            Job jobTF = Job.getInstance(conf, "tf_engine");
            jobTF.setJarByClass(IndexEngine.class);
            jobTF.setMapperClass(MapperTF.class);
            jobTF.setReducerClass(ReducerTF.class);

            jobTF.setMapOutputKeyClass(IntWritable.class);
            jobTF.setMapOutputValueClass(IntWritable.class);
            jobTF.setOutputKeyClass(IntWritable.class);
            jobTF.setOutputValueClass(MapWritable.class);
            jobTF.setInputFormatClass(TextInputFormat.class);
            jobTF.setOutputFormatClass(SequenceFileOutputFormat.class);

            return jobTF;
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
}
