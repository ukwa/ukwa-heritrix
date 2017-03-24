/**
 * 
 */
package uk.bl.wap.modules.uriuniqfilters;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.archive.modules.CoreAttributeConstants;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import uk.bl.wap.util.OutbackCDXClient;

/**
 * 
 * This checks OutbackCDX to see the last time we visited this URL.
 * 
 * It if was within the TTL, we refuse it and so prevent it from being
 * re-queued.
 * 
 * Needs some kind of caching for commonly seen URIs (for speed) and to avoid
 * queueing URLs that are not yet in OutbackCDX (because that's not done here).
 * 
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class OutbackCDXRecentlySeenUriUniqFilter
        extends RecentlySeenUriUniqFilter {

    private static final long serialVersionUID = 361526253773091309L;

    private OutbackCDXClient ocdx = new OutbackCDXClient();

    public OutbackCDXRecentlySeenUriUniqFilter() {
    }

    // Create a suitable LRU cache to avoid hitting the OutbackCDX back end too
    // hard, and to act as a short-term cache before OutbackCDX gets populated:
    private LoadingCache<String, Long> times = CacheBuilder.newBuilder()
            .maximumSize(10000) // Act as LRU cache
            .expireAfterAccess(6, TimeUnit.HOURS)
            // .expireAfterWrite(10, TimeUnit.DAYS)
            // .removalListener(MY_LISTENER)
            .build(new CacheLoader<String, Long>() {
                public Long load(String url) {
                    HashMap<String, Object> info;
                    try {
                        info = ocdx.getLastCrawl(url);
                        System.out.println("INFO: " + info);
                        long ms_ts = (long) info
                                .get(CoreAttributeConstants.A_FETCH_BEGAN_TIME);
                        return ms_ts / 1000;
                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }

                    // If it's unknown or went wrong, return a very very old
                    // timestamp:
                    return 0l;
                }
            });

    // private RemovalListener<? super Object, ? super Object> MY_LISTENER;

    /* (non-Javadoc)
     * @see uk.bl.wap.util.RecentlySeenUriUniqFilter#setAddWithTTL(java.lang.String, java.lang.String, int)
     */
    @Override
    public boolean setAddWithTTL(String key, String uri, int ttl_s) {
        try {
            Long ts = this.times.get(uri);
            long currentTime = System.currentTimeMillis() / 1000;
            if (currentTime - ts > ttl_s) {
                return false;
            } else {
                this.times.put(uri, currentTime);
                return true;
            }
        } catch (ExecutionException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return true;
    }

    /* (non-Javadoc)
     * @see org.archive.crawler.util.SetBasedUriUniqFilter#setRemove(java.lang.CharSequence)
     */
    @Override
    protected boolean setRemove(CharSequence key) {
        return false;
    }

    /* (non-Javadoc)
     * @see org.archive.crawler.util.SetBasedUriUniqFilter#setCount()
     */
    @Override
    protected long setCount() {
        return 0;
    }

}
