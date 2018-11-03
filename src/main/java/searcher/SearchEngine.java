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
            System.out.println("Arguments amount is incorrect. Add parameters like: [out path] [amount of docks] [query text]");
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
//
//    public static class TokenizerMapper
//            extends Mapper<Object, Text, Text, IntWritable> {
//
//        static enum CountersEnum {INPUT_WORDS}
//
//        private final static IntWritable one = new IntWritable(1);
//        private Text word = new Text();
//
//        private boolean caseSensitive;
//        private Set<String> patternsToSkip = new HashSet<String>();
//
//        private Configuration conf;
//        private BufferedReader fis;
//
//        @Override
//        public void setup(Context context) throws IOException,
//                InterruptedException {
//            conf = context.getConfiguration();
//            caseSensitive = conf.getBoolean("wordcount.case.sensitive", true);
//            if (conf.getBoolean("wordcount.skip.patterns", false)) {
//                URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
//                for (URI patternsURI : patternsURIs) {
//                    Path patternsPath = new Path(patternsURI.getPath());
//                    String patternsFileName = patternsPath.getName().toString();
//                    parseSkipFile(patternsFileName);
//                }
//            }
//        }
//
//        private void parseSkipFile(String fileName) {
//            try {
//                fis = new BufferedReader(new FileReader(fileName));
//                String pattern = null;
//                while ((pattern = fis.readLine()) != null) {
//                    patternsToSkip.add(pattern);
//                }
//            } catch (IOException ioe) {
//                System.err.println("Caught exception while parsing the cached file '"
//                        + StringUtils.stringifyException(ioe));
//            }
//        }
//
//        @Override
//        public void map(Object key, Text value, Context context
//        ) throws IOException, InterruptedException {
//            String line = (caseSensitive) ?
//                    value.toString() : value.toString().toLowerCase();
//            for (String pattern : patternsToSkip) {
//                line = line.replaceAll(pattern, "");
//            }
//            StringTokenizer itr = new StringTokenizer(line);
//            while (itr.hasMoreTokens()) {
//                word.set(itr.nextToken());
//                context.write(word, one);
//                Counter counter = context.getCounter(CountersEnum.class.getName(),
//                        CountersEnum.INPUT_WORDS.toString());
//                counter.increment(1);
//            }
//        }
//    }
//
//    public static class IntSumReducer
//            extends Reducer<Text, IntWritable, Text, IntWritable> {
//        private IntWritable result = new IntWritable();
//
//        public void reduce(Text key, Iterable<IntWritable> values,
//                           Context context
//        ) throws IOException, InterruptedException {
//            int sum = 0;
//            for (IntWritable val : values) {
//                sum += val.get();
//            }
//            result.set(sum);
//            context.write(key, result);
//        }
//    }
//
//    public static void main(String[] args) throws Exception {
//        Configuration conf = new Configuration();
//        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
//        String[] remainingArgs = optionParser.getRemainingArgs();
//        if ((remainingArgs.length != 2) && (remainingArgs.length != 4)) {
//            System.err.println("Usage: <command> <in> <out> [-skip skipPatternFile]");
//            System.exit(2);
//        }
//        Job job = Job.getInstance(conf, "search_engine");
//        job.setJarByClass(SearchEngine.class);
//        job.setMapperClass(TokenizerMapper.class);
//        job.setCombinerClass(IntSumReducer.class);
//        job.setReducerClass(IntSumReducer.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(IntWritable.class);
//
//        List<String> otherArgs = new ArrayList<String>();
//        for (int i = 0; i < remainingArgs.length; ++i) {
//            if ("-skip".equals(remainingArgs[i])) {
//                job.addCacheFile(new Path(remainingArgs[++i]).toUri());
//                job.getConfiguration().setBoolean("search_engine.skip.patterns", true);
//            } else {
//                otherArgs.add(remainingArgs[i]);
//            }
//        }
//        FileInputFormat.addInputPath(job, new Path(otherArgs.get(0)));
//        File tmpDir = new File(args[1]);
//        boolean exists = tmpDir.exists();
//        if (exists) {
//            FileUtils.deleteDirectory(tmpDir);
//        }
//        FileOutputFormat.setOutputPath(job, new Path(otherArgs.get(1)));
//
//        System.exit(job.waitForCompletion(true) ? 0 : 1);
//    }
}