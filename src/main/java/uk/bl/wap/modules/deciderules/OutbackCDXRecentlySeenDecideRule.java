/**
 * 
 */
package uk.bl.wap.modules.deciderules;

import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import uk.bl.wap.util.OutbackCDXClient;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class OutbackCDXRecentlySeenDecideRule
        extends RecentlySeenDecideRule {

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


    /*
     * (non-Javadoc)
     * 
     * @see uk.bl.wap.modules.deciderules.RecentlySeenDecideRule#getInfo()
     */
    @Override
    protected HashMap<String, Object> getInfo(String url) {
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

}
