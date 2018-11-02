import indexer.IndexEngine;
import searcher.SearchEngine;

import java.util.Arrays;

public class Main {
    private static final String INDEXER = "Indexer";
    private static final String SEARCHER = "Query";

    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Wrong function call. See readme.md");
            System.out.println(Arrays.toString(args));
            return;
        }
        String func = args[0];
        if (func.equals(INDEXER)) {
            try {
                IndexEngine.main(Arrays.copyOfRange(args, 1, args.length));
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else if (func.equals(SEARCHER)) {
            try {
                SearchEngine.main(Arrays.copyOfRange(args, 1, args.length));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
