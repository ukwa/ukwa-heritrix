package uk.bl.wap.util;

import java.io.IOException;
import java.io.Serializable;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.httpclient.URIException;
import org.archive.crawler.util.SetBasedUriUniqFilter;
import org.archive.wayback.UrlCanonicalizer;
import org.archive.wayback.surt.SURTTokenizer;
import org.archive.wayback.util.url.AggressiveUrlCanonicalizer;
import org.springframework.beans.factory.InitializingBean;

/**
 * 
 * This UriUniqFilter or 'Already Seen URIs' filter uses an off-the-shelf cache
 * engine that supports time-to-live caching. i.e. you can set the cached
 * entries to expire, so the crawler will 'forget' it has seen the URI and allow
 * it to be re-queued for crawling.
 * 
 * This can be used to support regular crawling at arbitrary times of day, with
 * SURT prefixes used to map URLs to TTL. Therefore the same crawl can contain
 * URLs that are visited at varying frequencies.
 * 
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public abstract class RecentlySeenUriUniqFilter extends SetBasedUriUniqFilter
        implements Serializable, InitializingBean {
    private static final long serialVersionUID = 1061526253773091309L;

    private static Logger LOGGER = Logger
            .getLogger(RecentlySeenUriUniqFilter.class.getName());

    public static final int HOUR = 60 * 60;
    public static final int DAY = HOUR * 24;
    public static final int WEEK = DAY * 7;
    public int defaultTTL = 4 * WEEK;

    private UrlCanonicalizer canonicalizer = new AggressiveUrlCanonicalizer();
    private WatchedFileSurtMap<Integer> ttlMap = new WatchedFileSurtMap<Integer>();

    /**
     * Default constructor
     */
    public RecentlySeenUriUniqFilter() {
        super();
    }

    /**
     * Initializer.
     */
    public void afterPropertiesSet() {
        try {
            this.ttlMap.init();
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE,
                    "Exception when initialising SURT-to-TTL map.", e);
        }
    }

    /**
     * @return the canonicalizer
     */
    public UrlCanonicalizer getCanonicalizer() {
        return canonicalizer;
    }

    /**
     * @param canonicalizer
     *            the canonicalizer to set
     */
    public void setCanonicalizer(UrlCanonicalizer canonicalizer) {
        this.canonicalizer = canonicalizer;
    }

    /**
     * @return the ttlMap
     */
    public WatchedFileSurtMap<Integer> getTtlMap() {
        return ttlMap;
    }

    /**
     * @param ttlMap
     *            the ttlMap to set
     */
    public void setTtlMap(WatchedFileSurtMap<Integer> ttlMap) {
        this.ttlMap = ttlMap;
    }

    /**
     * @return the defaultTTL
     */
    public int getDefaultTTL() {
        return defaultTTL;
    }

    /**
     * @param defaultTTL
     *            the defaultTTL to set
     */
    public void setDefaultTTL(int defaultTTL) {
        this.defaultTTL = defaultTTL;
    }

    /**
     * 
     * Use the map to look up the TTL for this Url.
     * 
     * @param url
     * @return TTL (in seconds)
     */
    protected Integer getTTLForUrl(String url) {
        try {
            SURTTokenizer st = new SURTTokenizer(url,
                    canonicalizer.isSurtForm());
            while (true) {
                String nextSearch = st.nextSearch();
                if (nextSearch == null) {
                    break;
                }
                LOGGER.fine("TTL-MAP:Checking " + nextSearch);
                if (ttlMap.get().containsKey(nextSearch)) {
                    Integer ttl = ttlMap.get().get(nextSearch);
                    LOGGER.info("TTL-MAP: \"" + ttl + "\" (" + url + ")");
                    return ttl;
                }
            }
        } catch (URIException e) {
            LOGGER.warning(e.toString());
        }
        return this.defaultTTL;
    }

}
