package uk.bl.wap.crawler.processor;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.http.HttpHeaders;
import org.archive.modules.CrawlURI;
import org.archive.modules.Processor;

/**
 * Annotates outlinks to ensure crawl identifier is passed on:
 * 
 * @author Andy Jackson
 */

public class CrawlIdAnnotator extends Processor {
    private final static Logger LOGGER = Logger
            .getLogger(CrawlIdAnnotator.class
            .getName());

    @Override
    protected boolean shouldProcess(CrawlURI curi) {
        return true;
    }

    @Override
    protected void innerProcess(CrawlURI curi) {
        try {
            LOGGER.info("Looking for crawlId...");
            // To spot a redirect...
            LOGGER.info("Location: "
                    + curi.getHttpResponseHeader(HttpHeaders.LOCATION));
            // NOTE that url can be relative (?)
            String crawlId = null;
            for (String k : curi.getData().keySet()) {
                LOGGER.info("DATA  " + k + " " + curi.getData().get(k));
            }
            if (curi.getData().containsKey("crawlId")) {
                crawlId = "crawlId:" + (String) curi.getData().get("crawlId");
                curi.getAnnotations().add(crawlId);
            } else {
                for (String anno : curi.getAnnotations()) {
                    if (anno.startsWith("crawlId:")) {
                        crawlId = anno;
                        break;
                    }
                }
            }
            // If we found a crawl ID, pass it on as an annotation:
            if (crawlId != null) {
                LOGGER.info("Found crawlId: " + crawlId);
                for (CrawlURI cout : curi.getOutLinks()) {
                    cout.getAnnotations().add(crawlId);
                }
            }

            // And the source:
            if (curi.getSourceTag() != null) {
                for (CrawlURI cout : curi.getOutLinks()) {
                    cout.setSourceTag(curi.getSourceTag());
                }
            }
        } catch (Exception e) {
            LOGGER.log(Level.WARNING,
                    "Problem annotating outlinks: " + curi.getURI(), e);
            curi.getNonFatalFailures().add(e);
        }
    }
}
