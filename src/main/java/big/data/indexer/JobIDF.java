//package big.data.indexer;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.io.*;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
//
//import java.io.IOException;
//
//public class JobIDF {
//    public static class ReducerIDF extends Reducer<IntWritable, IntWritable, Text, IntWritable> {
//        private final IntWritable ONE = new IntWritable(1);
//
//        public void reduce(IntWritable key, final Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
//            int sum = 0;
//            for (IntWritable ignored : values) {
//                sum++;
//            }
//            String tmp = key.toString() + "_" + sum;
//            context.write(new Text(tmp), ONE);
//            System.out.print(" ");
//        }
//    }
//
//    public static class MapperIDF extends Mapper<IntWritable, MapWritable, IntWritable, IntWritable> {
//        private final IntWritable ONE = new IntWritable(1);
//
//        @Override
//        public void map(IntWritable key, MapWritable value, Context context) throws IOException, InterruptedException {
//            for (Writable i : value.keySet()) {
//                context.write((IntWritable) i, ONE);
//            }
//        }
//    }
//
//
//    static Job getJob(Configuration conf) throws Exception {
//        try {
//            Job jobIDF = Job.getInstance(conf, "idf_engine");
//            jobIDF.setJarByClass(IndexEngine.class);
//            jobIDF.setMapperClass(MapperIDF.class);
//            jobIDF.setReducerClass(ReducerIDF.class);
//            jobIDF.setInputFormatClass(SequenceFileInputFormat.class);
//
//            jobIDF.setMapOutputKeyClass(IntWritable.class);
//            jobIDF.setMapOutputValueClass(IntWritable.class);
//
//            jobIDF.setOutputKeyClass(IntWritable.class);
//            jobIDF.setOutputValueClass(Text.class);
//
//            jobIDF.setOutputFormatClass(TextOutputFormat.class);
//
//            return jobIDF;
//        } catch (Exception ex) {
//            ex.printStackTrace();
//            throw ex;
//        }
//    }
//
//    //
//    public static class MapperIDFFin extends Mapper<Text, IntWritable, IntWritable, IntWritable> {
//
//        @Override
//        protected void setup(Context context) throws IOException, InterruptedException {
//            System.out.println("Hello_setup");
//            super.setup(context);
//            System.out.println("Hello_setup_finished");
//        }
//
//
//        @Override
//        public void run(Context context) throws IOException, InterruptedException {
//            System.out.println("Hello_run");
//            super.run(context);
//        }
//
//        @Override
//        public void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
//            System.out.println("Hello");
////            System.out.println("Key: " + key);
////            System.out.println("Value: " + value);
//        }
//    }
//
//    static Job getFinJob(Configuration conf) throws Exception {
//        try {
//            Job jobIDFFin = Job.getInstance(conf);
//            jobIDFFin.setJobName("idf_fin_engine");
//            jobIDFFin.setJarByClass(IndexEngine.class);
//            jobIDFFin.setMapperClass(MapperIDFFin.class);
//            jobIDFFin.setInputFormatClass(TextInputFormat.class);
//            jobIDFFin.setNumReduceTasks(0);
//
//            jobIDFFin.setMapOutputKeyClass(IntWritable.class);
//            jobIDFFin.setMapOutputValueClass(IntWritable.class);
//            jobIDFFin.setOutputFormatClass(TextOutputFormat.class);
//
//            return jobIDFFin;
//        } catch (Exception ex) {
//            ex.printStackTrace();
//            throw ex;
//        }
//    }
//}
