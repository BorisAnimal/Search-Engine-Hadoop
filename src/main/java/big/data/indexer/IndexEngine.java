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


    private static String getIDF(String filename) throws FileNotFoundException {
        Scanner sc = new Scanner(new File(filename));
        JSONObject json = new JSONObject();
        while (sc.hasNext()) {
            json.put(sc.nextInt() + "", sc.nextInt());
        }
        sc.close();
        return json.toString();
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

    private final static String TF_PATH = "output_tf";
    private final static String IDF_PATH = "output_idf";
    private final static String TMP_PATH1 = "output_tmp";
    private final static String TMP_PATH2 = "output_kek";


    /**
     * 1) Calculate TF for docks
     * 2) Based on TF, calculate idf for words
     * 3) Calculate TFIDF for docks
     */


    public static void main(String[] args) throws Exception {
        // 1) Calculate TF for docks

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
            // 2) Based on TF, calculate idf for words
            Job coolJob = CoolIDF.getJob(conf);
            FileInputFormat.addInputPath(coolJob, new Path(TF_PATH));
            deleteDir(IDF_PATH);
            FileOutputFormat.setOutputPath(coolJob, new Path(IDF_PATH));
            // run
            resCode = (coolJob.waitForCompletion(true) ? 0 : 1);
            System.out.println("IDF fin result: " + resCode);
            if (resCode == 0) {
                // 3) Calculate TFIDF for docks
                conf.set("idf", getIDF(IDF_PATH + "/part-r-00000"));
                Job jobTFIDF = JobTFIDF.getJob(conf);
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