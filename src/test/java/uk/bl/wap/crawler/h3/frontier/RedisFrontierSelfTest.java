package uk.bl.wap.crawler.h3.frontier;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class RedisFrontierSelfTest extends SelfTestBase {

    final private static Set<String> EXPECTED = Collections
            .unmodifiableSet(new HashSet<String>(Arrays.asList(
                    new String[] { "index.html", "link1.html", "link2.html",
                            "link3.html", "robots.txt", "favicon.ico" })));

    @Override
    protected void verify() throws Exception {
        Set<String> files = filesInArcs();
        assertEquals(EXPECTED, files);
    }
}
