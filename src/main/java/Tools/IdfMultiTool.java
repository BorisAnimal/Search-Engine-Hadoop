package Tools;

import org.apache.commons.io.FileUtils;
import org.json.JSONObject;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

public class IdfMultiTool {

    public static boolean isCaseSensitive() {
        return false;
    }

    public static String getIdfFile() {
        return "output_idf";
    }


    public static String getSkipPattern() {
        return "[^A-Za-z0-9 ]";
    }


    public static void deleteDir(String path) {
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

    private static JSONObject idfs = null;

    private static JSONObject loadIdfs() throws FileNotFoundException {
        if (idfs == null) {
            Scanner sc = new Scanner(new File(getIdfFile() + "/part-r-00000"));
            idfs = new JSONObject();
            while (sc.hasNext()) {
                idfs.put(sc.nextInt() + "", sc.nextInt());
            }
            sc.close();
        }
        assert idfs != null;
        return idfs;
    }

    public static String getIdfAsString() throws FileNotFoundException {
        return loadIdfs().toString();
    }

    public static Map<Integer, Integer> parseStringToMap(String param) {
        Map<Integer, Integer> idf = new HashMap<Integer, Integer>();

        JSONObject tmp = new JSONObject(param);
        Map tmpMap = tmp.toMap();
        for (Object i : tmpMap.keySet()) {
            idf.put(new Integer(i + ""), new Integer(tmpMap.get(i) + ""));
        }
        return idf;
    }

    public static Map<Integer, Double> parseQueryStringToMap(String param) {
        Map<Integer, Double> idf = new HashMap<Integer, Double>();

        JSONObject tmp = new JSONObject(param);
        Map tmpMap = tmp.toMap();
        for (Object i : tmpMap.keySet()) {
            idf.put(new Integer(i + ""), new Double(tmpMap.get(i) + ""));
        }
        return idf;
    }

    public static Map<Integer, Integer> parseStringToMap() throws FileNotFoundException {
        return parseStringToMap(getIdfAsString());
    }

    public static String parseMapToJsonString(Map map) {
        return new JSONObject(map).toString();
    }
}
