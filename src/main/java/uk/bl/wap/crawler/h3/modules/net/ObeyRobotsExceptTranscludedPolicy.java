/**
 * 
 */
package uk.bl.wap.crawler.h3.modules.net;

import org.archive.modules.CrawlURI;
import org.archive.modules.net.ObeyRobotsPolicy;
import org.archive.modules.net.Robotstxt;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class ObeyRobotsExceptTranscludedPolicy extends ObeyRobotsPolicy {

    /*
     * (non-Javadoc)
     * 
     * @see org.archive.modules.net.ObeyRobotsPolicy#allows(java.lang.String,
     * org.archive.modules.CrawlURI, org.archive.modules.net.Robotstxt)
     */
    @Override
    public boolean allows(String userAgent, CrawlURI curi,
            Robotstxt robotstxt) {
        // If this is an embed or transclusion , override robots.txt.
        if ("E".equals(curi.getLastHop()) || "I".equals(curi.getLastHop())) {
            return true;
        }
        // Otherwise, obey robots.txt:
        return super.allows(userAgent, curi, robotstxt);
    }

}
