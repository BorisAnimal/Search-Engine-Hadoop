package Tools;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.io.FileUtils;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.*;

public class IdfMultiTool {
    // Singleton object of idfs, common for all tasks
    private static JsonObject idfs = null;

    public static boolean isCaseSensitive() {
        return false;
    }

    // This file needed for both: Searching and Indexing
    public static String getIdfFile() {
        return "/home/team10/output_idf";
    }


    // Leave only matter symbols in input texts
    public static String getSkipPattern() {
        return "[^A-Za-z0-9 ]";
    }


    // Delete dir if it exists
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


    // WARNING! assumption, that idfs file only one generates
    private static JsonObject loadIdfs() throws FileNotFoundException {
        if (idfs == null) {
            Scanner sc = new Scanner(new File(getIdfFile() + "/part-r-00000"));
            idfs = new JsonObject();
            while (sc.hasNext()) {
                idfs.addProperty(sc.nextInt() + "", sc.nextInt());
            }
            sc.close();
        }
        assert idfs != null;
        return idfs;
    }


    // Parses content of file to String
    public static String getIdfAsString() throws FileNotFoundException {
        return loadIdfs().toString();
    }


    public static Map<Integer, Integer> parseStringToMap(String param) {
        Map<Integer, Integer> idf = new HashMap<Integer, Integer>();

        JsonParser parser = new JsonParser();
        JsonObject tmp = parser.parse(param).getAsJsonObject();
        Map tmpMap = jsonToMap(tmp);
        for (Object i : tmpMap.keySet()) {
            idf.put(new Integer(i + ""), new Integer(tmpMap.get(i) + ""));
        }
        return idf;
    }

    private static Map jsonToMap(JsonObject json) {
        Gson gson = new Gson();
        Type type = new TypeToken<Map<Integer, Integer>>() {
        }.getType();
        HashMap<Integer, Integer> map = (HashMap<Integer, Integer>) gson.fromJson(json, type);
        return map;
    }


    public static Map<Integer, Double> parseQueryStringToMap(String param) {
        Map<Integer, Double> idf = new HashMap<Integer, Double>();

        JsonParser parser = new JsonParser();
        JsonObject tmp = parser.parse(param).getAsJsonObject();
        Map tmpMap = jsonToMap(tmp);
        for (Object i : tmpMap.keySet()) {
            idf.put(new Integer(i + ""), new Double(tmpMap.get(i) + ""));
        }
        return idf;
    }

    public static Map<Integer, Integer> parseStringToMap() throws FileNotFoundException {
        return parseStringToMap(getIdfAsString());
    }

    public static String parseMapToJsonString(Map map) {
        return new Gson().toJson(map);
    }
}
