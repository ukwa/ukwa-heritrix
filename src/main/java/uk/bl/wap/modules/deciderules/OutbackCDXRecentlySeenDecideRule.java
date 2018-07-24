/**
 * 
 */
package uk.bl.wap.modules.deciderules;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.archive.modules.CoreAttributeConstants;
import org.archive.modules.CrawlURI;
import org.archive.modules.recrawl.FetchHistoryHelper;
import org.archive.modules.recrawl.RecrawlAttributeConstants;
import org.archive.util.Reporter;

import uk.bl.wap.util.OutbackCDXClient;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class OutbackCDXRecentlySeenDecideRule
        extends RecentlySeenDecideRule implements Reporter {

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

    private long errors = 0;
    private long hits = 0;
    private long misses = 0;

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
        errors++;
        return null;
    }

    /* (non-Javadoc)
     * @see uk.bl.wap.modules.deciderules.RecentlySeenDecideRule#evaluateWithTTL(java.lang.String, org.archive.modules.CrawlURI, int)
     */
    @Override
    public boolean evaluateWithTTL(String key, CrawlURI curi, int ttl_s) {
        // Get the last crawl date and hash:
        HashMap<String, Object> info = getInfo(curi.getURI());

        // Now determine whether we've seen this URL recently:
        Long ts;
        if (info == null || !info
                .containsKey(CoreAttributeConstants.A_FETCH_BEGAN_TIME)) {
            misses++;
            ts = 0l;
        } else {
            hits++;
            long ms_ts = (long) info
                    .get(CoreAttributeConstants.A_FETCH_BEGAN_TIME);
            ts = ms_ts / 1000;
            // Also store the crawl history to enable de-duplication:
            Long o_ts = (Long) info.get(FetchHistoryHelper.A_TIMESTAMP);
            if (o_ts != null) {
                // This is an attempt to ensure the fetch history is persisted
                // if this curi gets 'swapped out to disk'.
                // This is usually done in FetchHistoryProcessor, but as we are
                // looking up fetch history in the candidates
                // chain we need to make sure this happens early enough. Usually
                // this is all done during the fetch chain,
                // but here things can linger in the Frontier, so if they get
                // swapped out, it'll get lost.
                curi.addPersistentDataMapKey(
                        RecrawlAttributeConstants.A_FETCH_HISTORY);
                Map<String, Object> history = FetchHistoryHelper
                        .getFetchHistory(curi, o_ts, 1);
                if (history != null)
                    history.putAll(info);
                LOGGER.finest("Now have history: " + history);
            }
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

    /* Reporter */

    @Override
    public void reportTo(PrintWriter writer) throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public void shortReportLineTo(PrintWriter pw) throws IOException {
        pw.println("OutbackCDX Recently Seen Decide Rule Report Line");
    }

    @Override
    public Map<String, Object> shortReportMap() {
        return null;
    }

    @Override
    public String shortReportLegend() {
        return "OutbackCDX Recently Seen Decide Rule Report Legend";
    }

}
