/**
 * 
 */
package uk.bl.wap.modules.deciderules;

import java.util.logging.Logger;

import org.archive.modules.CrawlURI;
import org.archive.modules.deciderules.PredicatedDecideRule;
import org.archive.spring.KeyedProperties;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public abstract class RecentlySeenDecideRule extends PredicatedDecideRule {
    private static final Logger LOGGER = Logger
            .getLogger(RecentlySeenDecideRule.class.getName());

    /**
     * 
     */
    private static final long serialVersionUID = -8190739222291090071L;

    public static final int HOUR = 60 * 60;
    public static final int DAY = HOUR * 24;
    public static final int WEEK = DAY * 7;

    // Hash function used for building keys:
    private HashFunction hf = Hashing.murmur3_128();

    // Whether to use the hash of the URI rather than the URI (e.g. if
    // the implementation needs short keys)
    private boolean useHashedUriKey = false;

    protected KeyedProperties kp = new KeyedProperties();

    public KeyedProperties getKeyedProperties() {
        return kp;
    }

    /**
     * Default constructor
     */
    public RecentlySeenDecideRule() {
        super();
    }

    {
        setRecentlySeenTTLsecs(52 * WEEK);
    }

    /**
     * @return the TTL (seconds)
     */
    public int getRecentlySeenTTLsecs() {
        return (Integer) kp.get("recentlySeenTTLsecs");
    }

    /**
     * @param recentlySeenTTLsecs
     *            the TTL (in seconds) to set
     */
    public void setRecentlySeenTTLsecs(int recentlySeenTTLsecs) {
        LOGGER.warning("Setting TTL to " + recentlySeenTTLsecs);
        kp.put("recentlySeenTTLsecs", recentlySeenTTLsecs);
    }

    /**
     * @return the useHashedUriKey
     */
    public boolean isUseHashedUriKey() {
        return useHashedUriKey;
    }

    /**
     * @param useHashedUriKey
     *            the useHashedUriKey to set
     */
    public void setUseHashedUriKey(boolean useHashedUriKey) {
        this.useHashedUriKey = useHashedUriKey;
    }

    /**
     * 
     * Use the map to look up the TTL for this Url.
     * 
     * @param url
     * @return TTL (in seconds)
     */
    private Integer getTTLForUrl(String url) {
        int ttl = this.getRecentlySeenTTLsecs();
        LOGGER.fine("For " + url + " got TTL(s) " + ttl);
        return ttl;
    }

    /* (non-Javadoc)
     * @see org.archive.modules.deciderules.PredicatedDecideRule#evaluate(org.archive.modules.CrawlURI)
     */
    @Override
    protected boolean evaluate(CrawlURI curi) {
        String uri = curi.getURI();
        String key;
        if (useHashedUriKey) {
            key = hf.hashBytes(uri.getBytes()).toString();
        } else {
            key = uri;
        }
        int ttl_s = getTTLForUrl(uri);
        //
        // Long ttl = (Long) curi.getData().get("TTL");
        //
        // Allow entries to expire after a while, defaults, ranges, etc,
        // surt-prefixed.
        return evaluateWithTTL(key, curi, ttl_s);
    }

    /**
     * 
     * @param key
     * @param uri
     * @param ttl_s
     * @return true if the item is new and is being added to the set of known
     *         URIs
     */
    abstract public boolean evaluateWithTTL(String key, CrawlURI curi,
            int ttl_s);

}
