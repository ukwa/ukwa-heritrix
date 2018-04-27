/**
 * 
 */
package uk.bl.wap.crawler.postprocessor;

import java.util.logging.Logger;

import org.archive.modules.CrawlURI;
import org.archive.modules.Processor;

/**
 * This module catches HTTP 429 Too Many Requests responses, and slows the
 * crawler down.
 * 
 * If a 'Retry-after' header is given, that is used to set the Crawl-delay, but
 * only if it specifies a delay as a decimal number of seconds (rather than a
 * Date).
 * 
 * See https://httpstatuses.com/429 and
 * https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Retry-After
 * 
 * It's likely that this should be merge into the standard DispositionProcessor
 * rather than being run after it. See
 * org.archive.crawler.postprocessor.DispositionProcessor
 * 
 * Note that the Politeness Delay is used after the disposition chain has
 * executed, see WorkQueueFrontier.processFinish() implementation.
 * 
 * https://github.com/internetarchive/heritrix3/blob/3892c17b56c409c10e46eb0951ca18296d3d31f2/engine/src/main/java/org/archive/crawler/frontier/WorkQueueFrontier.java#L989
 * 
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class Http429ResponseProcessor extends Processor {
    private static final Logger logger = Logger
            .getLogger(Http429ResponseProcessor.class.getName());

    /* (non-Javadoc)
     * @see org.archive.modules.Processor#shouldProcess(org.archive.modules.CrawlURI)
     */
    @Override
    protected boolean shouldProcess(CrawlURI uri) {
        // For HTTP 429 we want to try to slow down:
        if (uri.getFetchStatus() == 429) {
            return true;
        }
        return false;
    }

    /* (non-Javadoc)
     * @see org.archive.modules.Processor#innerProcess(org.archive.modules.CrawlURI)
     */
    @Override
    protected void innerProcess(CrawlURI uri) throws InterruptedException {
        // Get the current crawl delay settings:
        long currentDelay_ms = uri.getPolitenessDelay();

        // Set up the new value for the Delay:
        Long newDelay_ms = null;

        // Look for Retry-after header:
        String retryAfter = uri.getHttpResponseHeader("Retry-after");
        if (retryAfter != null) {
            try {
                // Parse the retry-after delay returned by the remote host:
                float newDelay_s = Float.parseFloat(retryAfter);
                newDelay_ms = (long) (1000.0 * newDelay_s);
                // But don't go even faster that we were already planning to go:
                if (newDelay_ms < currentDelay_ms) {
                    newDelay_ms = currentDelay_ms;
                }
            } catch (Exception e) {
                logger.warning("Could not parse Retry-after of " + retryAfter);
            }
        }

        // If not set by header, increase the current delay by 1 second:
        if (newDelay_ms == null) {
            newDelay_ms = currentDelay_ms + 1000;
        }

        // And set the delay:
        uri.setPolitenessDelay(newDelay_ms);
    }

}
