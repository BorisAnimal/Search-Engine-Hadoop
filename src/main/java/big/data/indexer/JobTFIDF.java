package big.data.indexer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.Arrays;

public class JobTFIDF {
    public static class MapperTFIDF extends Mapper<IntWritable, MapWritable, IntWritable, IntWritable> {
        private MapWritable idf;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            try {
                if (context.getCacheFiles() != null && context.getCacheFiles().length > 0) {
                    idf = new MapWritable();
                    FileSystem fs = FileSystem.get(context.getConfiguration());
                    Path path = new Path(context.getCacheFiles()[0].toString() + "/part-r-00000");
                    DataInputStream reader = new DataInputStream(fs.open(path));
                    idf.readFields(reader);
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
            System.out.println(Arrays.toString(idf.entrySet().toArray()));
        }
    }

    static Job getJob(Configuration conf, final String IDF_PATH) throws Exception {
        try {
            Job jobTFIDF = Job.getInstance(conf, "tfidf_engine");
            jobTFIDF.setJarByClass(IndexEngine.class);
            jobTFIDF.setMapperClass(MapperTFIDF.class);
            jobTFIDF.setInputFormatClass(SequenceFileInputFormat.class);

            jobTFIDF.setMapOutputKeyClass(IntWritable.class);
            jobTFIDF.setMapOutputValueClass(IntWritable.class);
            jobTFIDF.addCacheFile(new Path(IDF_PATH).toUri());

            return jobTFIDF;
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
}
