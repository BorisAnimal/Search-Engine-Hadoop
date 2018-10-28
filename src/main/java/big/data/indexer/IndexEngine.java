package big.data.indexer;

import java.io.*;
import java.net.URI;
import java.util.*;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;
import org.json.JSONException;
import org.json.JSONObject;

public class IndexEngine {


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

    private final static String TF_PATH = "output_tf";
    private final static String IDF_PATH = "output_idf";
    private final static String TMP_PATH1 = "output_tmp";
    private final static String TMP_PATH2 = "output_kek";


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration(false);
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);

        Job jobTF = JobTF.getJob(conf);
        // origin input path
        FileInputFormat.addInputPath(jobTF, new Path(args[0]));
        deleteDir(TF_PATH);
        // tmp output path
        FileOutputFormat.setOutputPath(jobTF, new Path(TF_PATH));
        // run
        int resCode = (jobTF.waitForCompletion(true) ? 0 : 1);
        System.out.println("TF result: " + resCode);


        if (resCode == 0) {
            Job jobIDF = JobIDF.getJob(conf);
            // tmp tf path
            FileInputFormat.addInputPath(jobIDF, new Path(TF_PATH));
            // tmp idf path
            deleteDir(TMP_PATH1);
            FileOutputFormat.setOutputPath(jobIDF, new Path(TMP_PATH1));
            // run
            resCode = (jobIDF.waitForCompletion(true) ? 0 : 1);
            System.out.println("IDF result: " + resCode);
            if (resCode == 0) {
                Job jobIDFFin = JobIDF.getFinJob(conf);
                FileInputFormat.addInputPath(jobIDFFin, new Path(TMP_PATH1));
                deleteDir(IDF_PATH);
                FileOutputFormat.setOutputPath(jobIDFFin, new Path(IDF_PATH));
                // run
                resCode = (jobIDFFin.waitForCompletion(true) ? 0 : 1);
                System.out.println("IDF fin result: " + resCode);
//            deleteDir(TMP_PATH);

                if (resCode == 0) {
                    Job jobTFIDF = JobTFIDF.getJob(conf, IDF_PATH);

                    // tmp tf pasth
                    FileInputFormat.addInputPath(jobTFIDF, new Path(TF_PATH));
                    deleteDir(args[1]);
                    // given output file
                    FileOutputFormat.setOutputPath(jobTFIDF, new Path(args[1]));
                    // run
                    resCode = (jobTFIDF.waitForCompletion(true) ? 0 : 1);
                    System.out.println("TFIDF result: " + resCode);
                }
            }
        }
    }
}