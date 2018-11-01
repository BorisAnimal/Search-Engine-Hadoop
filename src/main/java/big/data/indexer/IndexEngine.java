package big.data.indexer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import static big.data.Tools.IdfMultiTool.deleteDir;
import static big.data.Tools.IdfMultiTool.getIdfAsString;
import static big.data.Tools.IdfMultiTool.getIdfFile;


public class IndexEngine {


    private final static String TF_PATH = "output_tf";
    private final static String TMP_PATH1 = "output_tmp";


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
    }
}