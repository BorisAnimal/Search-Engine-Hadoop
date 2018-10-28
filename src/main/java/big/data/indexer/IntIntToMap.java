package big.data.indexer;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataOutputStream;
import java.io.IOException;

public class IntIntToMap extends FileOutputFormat<IntWritable, IntWritable> {

    protected static class IntIntWriter extends RecordWriter<IntWritable, IntWritable> {
        private DataOutputStream out;
        private MapWritable map;

        IntIntWriter(DataOutputStream out) throws IOException {
            this.out = out;
            map = new MapWritable();
        }


        /**
         * Assumed that there are no collisions in @key
         */
        public synchronized void write(IntWritable key, IntWritable value) throws IOException {
            map.put(key, value);
        }

        public synchronized void close(TaskAttemptContext job)
                throws IOException {
            try {
                map.write(out);
            } finally {
                out.close();
            }

        }
    }


    @Override
    public RecordWriter<IntWritable, IntWritable> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        Path file = getDefaultWorkFile(job, "");
        FileSystem fs = file.getFileSystem(job.getConfiguration());
        FSDataOutputStream fileOut = fs.create(file, false);
        return new IntIntWriter(fileOut);
    }
}
