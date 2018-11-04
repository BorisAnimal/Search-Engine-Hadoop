package searcher;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.util.*;


import static Tools.IdfMultiTool.*;


public class SearchEngine {

    private static final String SEARCH_OUT = "output_search";

    // Arguments structure: output_search 10 "query text"
    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.out.println("Arguments amount is incorrect. Add parameters like: Query [out path] [amount of docks] [query text]");
            return;
        }

        // Parse args
        String index_file = args[0];
        String outFile = args[1];
        int docksNum = Integer.parseInt(args[2]);
        String query = args[args.length - 1];

        // Prepare query
        Map queryIdfs = processQuery(query);
        String serialized = parseMapToJsonString(queryIdfs);
        System.out.println(serialized);
        if (queryIdfs.isEmpty()) {
            System.out.println("Such query fully not indexed. 404");
        }

        Configuration config = new Configuration(false);
        config.set("query", parseMapToJsonString(queryIdfs));


        Job job = SearchJob.getJob(config);
        FileInputFormat.addInputPath(job, new Path(index_file));
        deleteDir(SEARCH_OUT);
        // tmp output path
        FileOutputFormat.setOutputPath(job, new Path(SEARCH_OUT));
        // run
        int resCode = (job.waitForCompletion(true) ? 0 : 1);
        System.out.println("Search job result: " + resCode);


        String[] cmd = {
                "/bin/sh",
                "-c",
                String.format("cat output_search/part* | sort -n -k2 -r | head -n%d > %s", docksNum, outFile)
        };
        Process p = Runtime.getRuntime().exec(cmd);
        int res = p.waitFor();
        deleteDir(SEARCH_OUT);
        System.exit(res);
    }

    private static Map<Integer, Double> processQuery(String query) throws FileNotFoundException {
        HashMap<Integer, Double> res = new HashMap<Integer, Double>();
        Map<Integer, Integer> idf = parseStringToMap();
        query = (isCaseSensitive()) ? query : query.toLowerCase();
        query = query.replaceAll(getSkipPattern(), "");
        StringTokenizer itr = new StringTokenizer(query);
        while (itr.hasMoreTokens()) {
            String tmp = itr.nextToken();
            int hash = tmp.hashCode();
            // this word was indexed
            if (idf.containsKey(hash)) {
                // this word already in query
                if (res.containsKey(hash)) {
                    res.put(hash, res.get(hash) + 1.0 / idf.get(hash));
                } else {
                    res.put(hash, 1.0 / idf.get(hash));
                }
            } else {
                System.out.println(tmp + " not in idfs");
            }
        }
        return res;
    }
}