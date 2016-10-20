/**
 * 
 */
package uk.bl.wap.crawler.processor;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.logging.Logger;

import org.archive.modules.CrawlURI;
import org.archive.net.UURIFactory;
import org.archive.util.Recorder;
import org.junit.Ignore;
import org.junit.Test;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class ViralContentProcessorTest {
    private final static Logger LOGGER = Logger
            .getLogger(ViralContentProcessorTest.class.getName());

    @Test
    @Ignore // This will fail when no clamd is running/available.
    public void testViralContentProcessor()
            throws InterruptedException, FileNotFoundException, IOException {
        ViralContentProcessor vcp = new ViralContentProcessor();
        vcp.setClamdHost("localhost");
        vcp.setClamdPort(3310);
        vcp.setClamdTimeout(0);
        vcp.setStreamMaxLength(94371840);
        CrawlURI curi = new CrawlURI(
                UURIFactory.getInstance("http://example.org/eicar.com.txt"));
        curi.setRecorder(Recorder.wrapInputStreamWithHttpRecord(
                new File("target"), "http://example.org/",
                new FileInputStream(
                        new File("src/test/resources/eicar.com.txt")),
                "UTF-8"));
        vcp.innerProcess(curi);
        LOGGER.fine(vcp.report());
        assertTrue("", vcp.report().contains("Viruses found:   1"));
    }

}
