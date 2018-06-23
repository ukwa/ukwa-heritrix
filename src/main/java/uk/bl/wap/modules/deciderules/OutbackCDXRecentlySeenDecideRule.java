/**
 * 
 */
package uk.bl.wap.modules.deciderules;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.archive.modules.CoreAttributeConstants;
import org.archive.modules.CrawlURI;
import org.archive.modules.recrawl.FetchHistoryHelper;

import uk.bl.wap.util.OutbackCDXClient;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class OutbackCDXRecentlySeenDecideRule extends RecentlySeenDecideRule {

    private static final long serialVersionUID = 361526253773091309L;

    private static final Logger LOGGER = Logger
            .getLogger(OutbackCDXRecentlySeenDecideRule.class.getName());

    private OutbackCDXClient outbackCDXClient = new OutbackCDXClient();

    public OutbackCDXClient getOutbackCDXClient() {
        return outbackCDXClient;
    }

    public void setOutbackCDXClient(OutbackCDXClient outbackCDXClient) {
        this.outbackCDXClient = outbackCDXClient;
    }

    public OutbackCDXRecentlySeenDecideRule() {
    }

    private HashMap<String, Object> getInfo(String url) {
        int tries = 3;
        while (tries > 0) {
            try {
                HashMap<String, Object> info = outbackCDXClient
                        .getLastCrawl(url);
                LOGGER.finest(
                        "OutbackCDX.getLastCrawl for " + url + ": " + info);
                return info;
            } catch (Exception e) {
                tries++;
                LOGGER.log(Level.SEVERE,
                        "Exception when querying OutbackCDX for url: " + url,
                        e);
                LOGGER.warning("Sleeping for 30s before retrying.");
                try {
                    Thread.sleep(1000 * 30);
                } catch (InterruptedException e1) {
                    LOGGER.warning("Sleeping OutbackCDX client rudely awoken!");
                }
            }
        }
        return null;
    }

    /* (non-Javadoc)
     * @see uk.bl.wap.modules.deciderules.RecentlySeenDecideRule#evaluateWithTTL(java.lang.String, org.archive.modules.CrawlURI, int)
     */
    @Override
    public boolean evaluateWithTTL(String key, CrawlURI curi, int ttl_s) {
        // Get the last crawl date and hash:
        HashMap<String, Object> info = getInfo(curi.toString());

        // Now determine whether we've seen this URL recently:
        Long ts;
        if (info == null || !info
                .containsKey(CoreAttributeConstants.A_FETCH_BEGAN_TIME)) {
            ts = 0l;
        } else {
            long ms_ts = (long) info
                    .get(CoreAttributeConstants.A_FETCH_BEGAN_TIME);
            ts = ms_ts / 1000;
            // Also store the crawl history to enable de-duplication:
            Map<String, Object> history = FetchHistoryHelper.getFetchHistory(
                    curi, (Long) info.get(FetchHistoryHelper.A_TIMESTAMP), 1);
            if (history != null)
                history.putAll(info);
            LOGGER.finest("Now have history: " + history);

        }

        // If we've never seen this before, or if enough time has elapsed
        // since we last saw it:
        long currentTime = System.currentTimeMillis() / 1000;
        if (ts == 0l || currentTime - ts > ttl_s || curi.forceFetch()) {
            LOGGER.finest("Got elapsed: " + (currentTime - ts) + " versus TTL "
                    + ttl_s + " hence NOT recently seen.");
            return false;
        } else {
            LOGGER.finest("Got elapsed: " + (currentTime - ts) + " versus TTL "
                    + ttl_s + " hence recently seen.");
            return true;
        }
    }

}
