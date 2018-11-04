package indexer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import static Tools.IdfMultiTool.deleteDir;
import static Tools.IdfMultiTool.getIdfAsString;
import static Tools.IdfMultiTool.getIdfFile;


public class IndexEngine {


    private final static String TF_PATH = "/home/team10/output_tf";
    private final static String TMP_PATH1 = "home/team10/output_tmp";


    /**
     * 1) Calculate TF for docks
     * 2) Based on TF, calculate idf for words
     * 3) Calculate TFIDF for docks
     */


    public static void main(String[] args) throws Exception {
        // 1) Calculate TF for docks

        Configuration conf = new Configuration(false);
        conf.set("fs.hdfs.impl",
                org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
        );
        conf.set("fs.file.impl",
                org.apache.hadoop.fs.LocalFileSystem.class.getName()
        );
        Job jobTF = JobTF.getJob(conf);
        // origin input path
        System.out.println(args[0]);
        Path inputpath = new Path(args[0] );//+ "/" + "AA" + "*"
        FileInputFormat.addInputPath(jobTF, inputpath);
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
            deleteDir(getIdfFile());
            FileOutputFormat.setOutputPath(coolJob, new Path(getIdfFile()));
            // run
            resCode = (coolJob.waitForCompletion(true) ? 0 : 1);
            System.out.println("IDF fin result: " + resCode);
            if (resCode == 0) {
                // 3) Calculate TFIDF for docks
                conf.set("idf", getIdfAsString());
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
        deleteDir(TF_PATH);
        deleteDir(TMP_PATH1);
    }
}