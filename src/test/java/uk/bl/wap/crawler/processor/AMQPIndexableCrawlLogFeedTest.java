package uk.bl.wap.crawler.processor;

import static org.junit.Assert.assertEquals;

import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;

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
        assertEquals(
                "{\"id\":\"83de0f10-48e0-4ff9-8985-45f38f97b865\",\"args\":[],\"task\":\"crawl.tasks.index_uri\",\"kwargs\":{\"url\":\"http://www.bl.uk\"}}",
                jo.toString());
    }

}
