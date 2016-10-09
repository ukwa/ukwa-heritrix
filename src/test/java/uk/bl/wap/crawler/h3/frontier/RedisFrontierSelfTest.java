package uk.bl.wap.crawler.h3.frontier;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class RedisFrontierSelfTest extends SelfTestBase {

    static {
        System.setProperty("java.util.logging.SimpleFormatter.format",
                "%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS.%1$tL %4$-7s [%3$s] (%2$s) %5$s %6$s%n");

        final ConsoleHandler consoleHandler = new ConsoleHandler();
        consoleHandler.setLevel(Level.FINEST);
        consoleHandler.setFormatter(new SimpleFormatter());
        final Logger app = Logger.getGlobal();
        app.setLevel(Level.FINEST);
        app.addHandler(consoleHandler);
    }

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
