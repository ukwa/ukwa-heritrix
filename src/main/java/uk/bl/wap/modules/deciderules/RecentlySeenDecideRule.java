/**
 * 
 */
package uk.bl.wap.modules.deciderules;

import java.io.IOException;
import java.io.PrintWriter;
import java.text.ParseException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import org.archive.modules.CoreAttributeConstants;
import org.archive.modules.CrawlURI;
import org.archive.modules.deciderules.DecideResult;
import org.archive.modules.deciderules.PredicatedDecideRule;
import org.archive.modules.recrawl.FetchHistoryHelper;
import org.archive.spring.KeyedProperties;
import org.archive.util.DateUtils;
import org.archive.util.Reporter;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public abstract class RecentlySeenDecideRule extends PredicatedDecideRule
        implements Reporter {
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
    public static final String LAUNCH_TIMESTAMP = "launchTimestamp";

    /**
     * Refresh depth key, if set in CrawlURI, indicating how many levels deep
     * the launchTimestamp should go. i.e. how long the hop-path can get before
     * we prevent the launchTimestamp from being inherited.
     */
    public static final String REFRESH_DEPTH = "refreshDepth";

    private long errors = 0;
    private long hits = 0;
    private long misses = 0;

    // Hash function used for building keys:
    private HashFunction hf = Hashing.murmur3_128();

    // Whether to use the hash of the URI rather than the URI (e.g. if
    // the implementation needs short keys)
    private boolean useHashedUriKey = false;

    // Whether to pay attention to the forceFetch flag and treat then as
    // not-recently-seen.
    private boolean obeyForceFetch = false;

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

    {
        // Default to no launch time-stamp override set:
        // setLaunchTimestamp();
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
     * @return the obeyForceFetch
     */
    public boolean isObeyForceFetch() {
        return obeyForceFetch;
    }

    /**
     * Control whether URIs marked as 'forceFetch' will bypass the recently-seen
     * check.
     * 
     * @param obeyForceFetch
     *            the obeyForceFetch to set
     */
    public void setObeyForceFetch(boolean obeyForceFetch) {
        this.obeyForceFetch = obeyForceFetch;
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
        // Ensure the launch timestamp is inherited to the correct depth:
        setLaunchTimestampInheritance(curi);

        // Now perform the actual test:
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

        // Also pick up any launch timestamp:
        long launch_ts = chooseLaunchTimestamp(curi);
        return evaluateWithTTL(key, curi, ttl_s, launch_ts);
    }

    void setLaunchTimestampInheritance(CrawlURI curi) {
        // Look for refreshDepth setting:
        int refreshDepth = 0; // Seeds only by default
        if (curi.getData().containsKey(REFRESH_DEPTH)) {
            refreshDepth = (int) curi.getData().get(REFRESH_DEPTH);
        }
        LOGGER.finer("Found refreshDepth " + refreshDepth + " for " + curi
                + " hop count " + curi.getHopCount());

        // Check whether to inherit the launchTimestamp:
        if (refreshDepth == -1 || refreshDepth > curi.getHopCount()) {
            LOGGER.info("Ensuring launchTimestamp is inherited for " + curi);
            curi.makeHeritable(LAUNCH_TIMESTAMP);
        } else {
            LOGGER.finer(
                    "Ensuring launchTimestamp is NOT inherited for " + curi);
            curi.makeNonHeritable(LAUNCH_TIMESTAMP);
        }

    }

    /**
     * Look for launch timestamp in this bean (plus any sheet overrides), and
     * also in the CrawlURI
     * 
     * Choose whichever is the most recent.
     * 
     * @param curi
     * @return
     */
    long chooseLaunchTimestamp(CrawlURI curi) {
        // Allow launch-request timestamps, via sheet by default:
        long sheet_ts = this.parseLaunchTimestamp(this.getLaunchTimestamp());
        // Also allow URI-level override of the launch timestamp:
        long curi_is = -1;
        if (curi.getData().containsKey(LAUNCH_TIMESTAMP)) {
            curi_is = this.parseLaunchTimestamp(
                    (String) curi.getData().get(LAUNCH_TIMESTAMP));
        }
        // Look at both and choose the most recent (largest value):
        return Long.max(sheet_ts, curi_is);
    }

    protected long parseLaunchTimestamp(String launch_tss) {
        long launch_ts = -1;
        if (launch_tss != null ) {
            try {
                launch_ts = DateUtils
                        .parse14DigitDate(launch_tss)
                        .getTime();
                launch_ts = launch_ts / 1000;
            } catch (ParseException e) {
                LOGGER.severe("Could not parse launch timestamp field: "
                        + launch_tss);
            }
        }
        return launch_ts;
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
    private boolean evaluateWithTTL(String key, CrawlURI curi, int ttl_s,
            long launch_ts) {
        // Get the last crawl date and hash:
        HashMap<String, Object> info = getInfo(curi.getURI());

        // Now determine whether we've seen this URL recently:
        Long ts;
        if (info == null) {
            // No info at all means it went wrong
            errors++;
            ts = 0l;
        } else if (!info
                .containsKey(CoreAttributeConstants.A_FETCH_BEGAN_TIME)) {
            // Lookup worked but no match:
            misses++;
            ts = 0l;
        } else {
            // Match found:
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
                // curi.addPersistentDataMapKey(
                // RecrawlAttributeConstants.A_FETCH_HISTORY);
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
        if (obeyForceFetch && curi.forceFetch()) {
            LOGGER.finest(
                    "Marked as force-fetch, hence classifying as NOT recently seen "
                            + curi);
            return false;
        } else if (ts == 0l || (currentTime - ts) > ttl_s) {
            LOGGER.finest("Got elapsed: " + (currentTime - ts) + " versus TTL "
                    + ttl_s + " hence NOT recently seen " + curi);
            return false;
        } else if (launch_ts > 0 && ts < launch_ts) {
            LOGGER.finest("Got recrawl time: " + ts + " versus launch time "
                    + launch_ts + " hence NOT recently seen " + curi);
            return false;
        } else {
            LOGGER.finest("Has been recently seen: " + curi);
            return true;
        }
    }

    abstract protected HashMap<String, Object> getInfo(String url);

    /* Reporter */

    @Override
    public void reportTo(PrintWriter writer) throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public void shortReportLineTo(PrintWriter pw) throws IOException {
        pw.println("Recently Seen Decide Rule Report Line");
    }

    @Override
    public Map<String, Object> shortReportMap() {
        return null;
    }

    @Override
    public String shortReportLegend() {
        return "Recently Seen Decide Rule Report Legend";
    }

    /** Some helpers **/

    public static void addLaunchTimestamp(CrawlURI curi, Date date) {
        String launch_ts = DateUtils.get14DigitDate(date);
        // Add as a persistent field:
        // curi.addPersistentDataMapKey(RecentlySeenDecideRule.LAUNCH_TIMESTAMP);
        curi.getData().put(RecentlySeenDecideRule.LAUNCH_TIMESTAMP,
                launch_ts);
        // Also note in an annotation:
        curi.getAnnotations()
                .add(RecentlySeenDecideRule.LAUNCH_TIMESTAMP + ":" + launch_ts);
    }

}
