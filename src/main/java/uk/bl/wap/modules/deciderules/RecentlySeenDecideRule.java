/**
 * 
 */
package uk.bl.wap.modules.deciderules;

import java.text.ParseException;
import java.util.logging.Logger;

import org.archive.modules.CrawlURI;
import org.archive.modules.deciderules.DecideResult;
import org.archive.modules.deciderules.PredicatedDecideRule;
import org.archive.spring.KeyedProperties;
import org.archive.util.DateUtils;

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

    /** Recrawl-interval key if set in CrawlURI, in seconds */
    public static final String RECRAWL_INTERVAL = "recrawlInterval";

    /** Launch timestamp key if set in CrawlURI, in ms since epoch */
    public static final String LAUNCH_TS = "launch_ts";
    public static final String LAUNCH_TIMESTAMP = "launchTimestamp";

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
        setRecrawlInterval(52 * WEEK);
    }

    /**
     * @return the TTL (seconds)
     */
    public int getRecrawlInterval() {
        return (Integer) kp.get(RECRAWL_INTERVAL);
    }

    /**
     * @param recentlySeenTTLsecs
     *            the TTL (in seconds) to set
     */
    public void setRecrawlInterval(int recentlySeenTTLsecs) {
        LOGGER.info("Setting TTL to " + recentlySeenTTLsecs);
        kp.put(RECRAWL_INTERVAL, recentlySeenTTLsecs);
    }

    /**
     * @return the launch timestamp (wayback format 14 char)
     */
    public String getLaunchTimestamp() {
        return (String) kp.get(LAUNCH_TIMESTAMP);
    }

    /**
     * @param launchTimestamp
     *            the launch timestamp (wayback format 14 char) to set
     */
    public void setLaunchTimestamp(String launchTimestamp) {
        LOGGER.info("Setting launchTimestamp to " + launchTimestamp);
        kp.put(LAUNCH_TIMESTAMP, launchTimestamp);
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
        int ttl = this.getRecrawlInterval();
        LOGGER.fine("For " + url + " got TTL(s) " + ttl);
        return ttl;
    }

    /**
     * Used to determine whether this rule can change the current decision and
     * if not, skip the rule; if 'lookupEveryUri' is true, this rule will never
     * be skipped.
     * 
     * These rules can have useful side-effects (getting the previous hash) but
     * we only need to do that when the URL may be included.
     */
    @Override
    public DecideResult onlyDecision(CrawlURI uri) {
        if (getLookupEveryUri()) {
            return null;
        } else {
            return this.getDecision();
        }
    }

    /**
     * Whether to perform a lookup on every URI; default is true.
     */
    {
        setLookupEveryUri(true);
    }

    public void setLookupEveryUri(boolean lookupEveryUri) {
        kp.put("lookupEveryUri", lookupEveryUri);
    }

    public boolean getLookupEveryUri() {
        return (Boolean) kp.get("lookupEveryUri");
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
        // Allow entries to expire after a while, defaults, ranges, etc,
        // surt-prefixed.
        int ttl_s = getTTLForUrl(uri);
        // Allow per-curi override:
        if (curi.getData().containsKey(RECRAWL_INTERVAL)) {
            ttl_s = (int) curi.getData().get(RECRAWL_INTERVAL);
        }
        // Allow launch-request-time-stamps:
        String launch_tss = null;
        if (curi.getData().containsKey(LAUNCH_TS)) {
            launch_tss = (String) curi.getData().get(LAUNCH_TS);
        }
        // Also allow sheet override of the launch timestamp:
        if (this.getLaunchTimestamp() != null) {
            launch_tss = this.getLaunchTimestamp();
        }
        long launch_ts = -1;
        if (launch_tss != null ) {
            try {
                launch_ts = DateUtils
                        .parse14DigitDate(launch_tss)
                        .getTime();
                launch_ts = launch_ts / 1000;
            } catch (ParseException e) {
                LOGGER.severe("Could not parse launch timestamp field: "
                        + curi.getData().get(LAUNCH_TS));
            }
            
        }
        return evaluateWithTTL(key, curi, ttl_s, launch_ts);
    }

    /**
     * 
     * @param key
     * @param uri
     * @param ttl_s
     *            Recrawl period (time-to-live in seconds) if this has been
     *            crawled before:
     * @param launch_ts
     *            The target launch time, if any, as seconds since January 1,
     *            1970, 00:00:00 GMT.
     * 
     * @return true if the item is new and is being added to the set of known
     *         URIs
     */
    abstract public boolean evaluateWithTTL(String key, CrawlURI curi,
            int ttl_s, long launch_ts);

}
