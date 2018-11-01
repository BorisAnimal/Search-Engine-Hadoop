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

public class JobTF {
    public static class MapperTF extends Mapper<Object, Text, IntWritable, IntWritable> {
        private IntWritable whash = new IntWritable();
        private boolean caseSensitive = false;
        private Set<String> patternsToSkip = new HashSet<String>();

        private Configuration conf;
        private BufferedReader fis;

        @Override
        public void setup(Context context) throws IOException {
            conf = context.getConfiguration();
//            caseSensitive = conf.getBoolean("wordcount.case.sensitive", true);
            if (conf.getBoolean("wordcount.skip.patterns", false)) {
                URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
                for (URI patternsURI : patternsURIs) {
                    Path patternsPath = new Path(patternsURI.getPath());
                    String patternsFileName = patternsPath.getName().toString();
                    parseSkipFile(patternsFileName);
                }
            }
        }

        private void parseSkipFile(String fileName) {
            try {
                fis = new BufferedReader(new FileReader(fileName));
                String pattern = null;
                while ((pattern = fis.readLine()) != null) {
                    patternsToSkip.add(pattern);
                }
            } catch (IOException ioe) {
                System.err.println("Caught exception while parsing the cached file '"
                        + StringUtils.stringifyException(ioe));
            }
        }

        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = (caseSensitive) ? value.toString() : value.toString().toLowerCase();
            for (String pattern : patternsToSkip) {
                line = line.replaceAll(pattern, "");
            }

            String[] tuple = line.split("\\n");
            try {
                // For each document (JSON)
                for (String str : tuple) {
                    JSONObject obj = new JSONObject(str);
                    IntWritable d_id = new IntWritable(obj.getInt("id"));
                    String text = obj.getString("text");
                    System.out.println(text);
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
