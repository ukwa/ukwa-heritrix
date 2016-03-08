package uk.bl.wap.util;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class RedisRecentlySeenUriUniqFilterTest {

    @Test
    public void test() {
        String keyspaceInfo = "# Keyspace\ndb0:keys=1349,expires=1349,avg_ttl=31404911213";
        long count = RedisRecentlySeenUriUniqFilter
                .parseKeyspaceInfo(keyspaceInfo);
        assertEquals("number of keys not determined correctly!", 1349, count);
    }

}
