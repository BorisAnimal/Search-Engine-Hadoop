package big.data;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.util.*;


public class IndexEngine extends Configured implements Tool {

    public static class MapperIDF extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
        private static final IntWritable ONE = new IntWritable(1);
        private final transient IntWritable WORD_HASH = new IntWritable();

        private boolean caseSensitive = false;
        private Set<String> patternsToSkip = new HashSet<String>(); //TODO

        /**
         * Counts IDF for words
         */
        @Override
        public void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException {
//            final String line = value.toString();
//            final StringTokenizer tokenizer = new StringTokenizer(line);
//            while (tokenizer.hasMoreTokens()) {
//                word.set(tokenizer.nextToken());
//                context.write(word, ONE);
//            }

            // Preprocessing
            String line = (caseSensitive) ? value.toString() : value.toString().toLowerCase();
//            for (String pattern : patternsToSkip) {
//                line = line.replaceAll(pattern, "");
//            }

            String[] tuple = line.split("\\n");
            try {
                // For each document (JSON)
                for (String str : tuple) {
                    JSONObject obj = new JSONObject(str);

                    HashSet<Integer> hashes = new HashSet<Integer>();
                    final StringTokenizer itr = new StringTokenizer(obj.getString("text"));
                    while (itr.hasMoreTokens()) {
                        WORD_HASH.set(itr.nextToken().hashCode());
                    }
                    for (int i : hashes) {
                        System.out.println(i + "");
                        WORD_HASH.set(i);
                        context.write(WORD_HASH, ONE);
                    }
                }
            } catch (JSONException e) {
                e.printStackTrace();
            }
        }
    }


    public static class ReducerIDF extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

        @Override
        public void reduce(final IntWritable key, final Iterable<IntWritable> values, final Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (final IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }


    @Override
    public int run(final String[] args) throws Exception {
        final Configuration conf = this.getConf();
        final Job job = Job.getInstance(conf, "idf_counter");
        job.setJarByClass(IndexEngine.class);

        job.setMapperClass(MapperIDF.class);
        job.setReducerClass(ReducerIDF.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        File tmpDir = new File(args[1]);
        boolean exists = tmpDir.exists();
        if (exists) {
            FileUtils.deleteDirectory(tmpDir);
        }
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(final String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: <command> <in> <out>");
            System.exit(1);
        }

        final int returnCode = ToolRunner.run(new Configuration(), new IndexEngine(), args);
        System.exit(returnCode);
    }
}

