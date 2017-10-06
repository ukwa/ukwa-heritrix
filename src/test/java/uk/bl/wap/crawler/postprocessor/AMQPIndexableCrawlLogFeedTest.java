package uk.bl.wap.crawler.postprocessor;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;

import uk.bl.wap.crawler.postprocessor.AMQPIndexableCrawlLogFeed;

public class AMQPIndexableCrawlLogFeedTest {

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void test() {
        AMQPIndexableCrawlLogFeed clf = new AMQPIndexableCrawlLogFeed();
        JSONObject jo = new JSONObject();
        jo.put("url", "http://www.bl.uk");
        // And wrap:
        jo = clf.wrapForCelery(jo);
        jo.put("id", "83de0f10-48e0-4ff9-8985-45f38f97b865");

        JSONObject expected = new JSONObject(
                "{\"id\":\"83de0f10-48e0-4ff9-8985-45f38f97b865\",\"task\":\"crawl.tasks.index_uri\",\"args\":[],\"kwargs\":{\"url\":\"http://www.bl.uk\"}}");
        areEqual(expected, jo);
    }

    /*
     * CLumsily hacking in a sensible JSON comparison algorithm.
     * 
     * From
     * http://stackoverflow.com/questions/2253750/compare-two-json-objects-in-
     * java
     * 
     */

    public static boolean areEqual(Object ob1, Object ob2)
            throws JSONException {
        Object obj1Converted = convertJsonElement(ob1);
        Object obj2Converted = convertJsonElement(ob2);
        return obj1Converted.equals(obj2Converted);
    }

    private static Object convertJsonElement(Object elem) throws JSONException {
        if (elem instanceof JSONObject) {
            JSONObject obj = (JSONObject) elem;
            Iterator<String> keys = obj.keys();
            Map<String, Object> jsonMap = new HashMap<>();
            while (keys.hasNext()) {
                String key = keys.next();
                jsonMap.put(key, convertJsonElement(obj.get(key)));
            }
            return jsonMap;
        } else if (elem instanceof JSONArray) {
            JSONArray arr = (JSONArray) elem;
            Set<Object> jsonSet = new HashSet<>();
            for (int i = 0; i < arr.length(); i++) {
                jsonSet.add(convertJsonElement(arr.get(i)));
            }
            return jsonSet;
        } else {
            return elem;
        }
    }
}
