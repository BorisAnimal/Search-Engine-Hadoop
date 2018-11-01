package big.data.Tools;

import org.apache.commons.io.FileUtils;
import org.json.JSONObject;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class IdfDeserializer {

    public static String getIdfFile() {
        return "output_idf";
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

    public static String getIdfAsString() throws FileNotFoundException {
        Scanner sc = new Scanner(new File(getIdfFile() + "/part-r-00000"));
        JSONObject json = new JSONObject();
        while (sc.hasNext()) {
            json.put(sc.nextInt() + "", sc.nextInt());
        }
        sc.close();
        return json.toString();
    }

    public static Map<Integer, Integer> getIdfAsMap(String param) {
        Map<Integer, Integer> idf = new HashMap<Integer, Integer>();

        JSONObject tmp = new JSONObject(param);
        Map tmpMap = tmp.toMap();
        for (Object i : tmpMap.keySet()) {
            idf.put(new Integer(i + ""), new Integer(tmpMap.get(i) + ""));
        }
        return idf;
    }

    public static Map<Integer, Integer> getIdfAsMap() throws FileNotFoundException {
        return getIdfAsMap(getIdfAsString());
    }
}
