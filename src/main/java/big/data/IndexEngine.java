package big.data;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;
import org.json.JSONException;
import org.json.JSONObject;

public class IndexEngine {
    public static class MapperTF extends Mapper<Object, Text, IntWritable, IntWritable> {
        private IntWritable whash = new IntWritable();
        private boolean caseSensitive = true;
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

    private static void deleteDir(String path) {
        File tmpDir = new File(path);
        boolean exists = tmpDir.exists();
        if (exists) {
            try {
                FileUtils.deleteDirectory(tmpDir);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();
        if ((remainingArgs.length != 3) && (remainingArgs.length != 5)) {
            System.err.println("Usage: <command> <in> <out> [-skip skipPatternFile]");
            System.exit(2);
        }
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


        List<String> otherArgs = new ArrayList<String>();
        for (int i = 0; i < remainingArgs.length; ++i) {
            if ("-skip".equals(remainingArgs[i])) {
                jobTF.addCacheFile(new Path(remainingArgs[++i]).toUri());
                jobTF.getConfiguration().setBoolean("index_engine.skip.patterns", true);
            } else {
                otherArgs.add(remainingArgs[i]);
            }
        }
        FileInputFormat.addInputPath(jobTF, new Path(otherArgs.get(0)));
        deleteDir(args[1]);
        FileOutputFormat.setOutputPath(jobTF, new Path(otherArgs.get(1)));
        int resCode = (jobTF.waitForCompletion(true) ? 0 : 1);
        System.out.println("TF result: " + resCode);


        if (resCode == 0) {
            Job jobIDF = Job.getInstance(conf, "idf_engine");
            jobIDF.setJarByClass(IndexEngine.class);
            jobIDF.setMapperClass(MapperIDF.class);
            jobIDF.setReducerClass(ReducerIDF.class);
            jobIDF.setInputFormatClass(SequenceFileInputFormat.class);

            jobIDF.setMapOutputKeyClass(IntWritable.class);
            jobIDF.setMapOutputValueClass(IntWritable.class);

            jobIDF.setOutputKeyClass(IntWritable.class);
            jobIDF.setOutputValueClass(IntWritable.class);

            jobIDF.setOutputFormatClass(TextOutputFormat.class);

            FileInputFormat.addInputPath(jobIDF, new Path(otherArgs.get(1)));
            deleteDir(args[2]);
            FileOutputFormat.setOutputPath(jobIDF, new Path("output_idf"));
            resCode = (jobIDF.waitForCompletion(true) ? 0 : 1);
            System.out.println("IDF result: " + resCode);


        }
    }
}