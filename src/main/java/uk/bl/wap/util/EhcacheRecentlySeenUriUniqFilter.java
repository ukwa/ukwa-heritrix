/**
 * 
 */
package uk.bl.wap.util;

import java.util.logging.Logger;

import org.springframework.beans.factory.annotation.Required;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Ehcache;
import net.sf.ehcache.Element;
import net.sf.ehcache.Status;
import net.sf.ehcache.config.CacheConfiguration;
import net.sf.ehcache.config.Configuration;
import net.sf.ehcache.config.DiskStoreConfiguration;
import net.sf.ehcache.store.MemoryStoreEvictionPolicy;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class EhcacheRecentlySeenUriUniqFilter
        extends RecentlySeenUriUniqFilter {

    /** */
    private static final long serialVersionUID = 7156746218148487509L;

    private static Logger LOGGER = Logger
            .getLogger(EhcacheRecentlySeenUriUniqFilter.class.getName());

    private String cacheStorePath = System.getProperty("java.io.tmpdir");
    private CacheManager manager;
    private Ehcache cache;

    private int maxEntriesLocalHeap = 1000 * 1000;

    private int maxElementsOnDisk = 0;

    public EhcacheRecentlySeenUriUniqFilter() {
        super();
    }

    /**
     * 
     * @return
     */
    public String getCacheStorePath() {
        return cacheStorePath;
    }

    /**
     * 
     * @param cacheStorePath
     */
    @Required
    public void setCacheStorePath(String cacheStorePath) {
        this.cacheStorePath = cacheStorePath;
    }

    /**
     * 
     * @return
     */
    public Ehcache getCache() {
        if (!isCacheAvailable()) {
            setupCache();
        }
        return cache;
    }

    /**
     * 
     * @param cache
     */
    public void setCache(Ehcache cache) {
        this.cache = cache;
    }

    private boolean isCacheAvailable() {
        if (cache == null || !cache.getStatus().equals(Status.STATUS_ALIVE)) {
            return false;
        } else {
            return true;
        }
    }

    /**
     * @return the maxEntriesLocalHeap
     */
    public int getMaxEntriesLocalHeap() {
        return maxEntriesLocalHeap;
    }

    /**
     * @param maxEntriesLocalHeap
     *            the maxEntriesLocalHeap to set
     */
    public void setMaxEntriesLocalHeap(int maxEntriesLocalHeap) {
        this.maxEntriesLocalHeap = maxEntriesLocalHeap;
    }

    /**
     * @return the maxElementsOnDisk
     */
    public int getMaxElementsOnDisk() {
        return maxElementsOnDisk;
    }

    /**
     * @param maxElementsOnDisk
     *            the maxElementsOnDisk to set
     */
    public void setMaxElementsOnDisk(int maxElementsOnDisk) {
        this.maxElementsOnDisk = maxElementsOnDisk;
    }

    /**
     * 
     */
    private void setupCache() {
        LOGGER.info("Setting up Ehcache...");
        if (manager == null
                || !manager.getStatus().equals(Status.STATUS_ALIVE)) {
            Configuration configuration = new Configuration();
            DiskStoreConfiguration diskStoreConfiguration = new DiskStoreConfiguration();
            diskStoreConfiguration.setPath(cacheStorePath);
            // Already created a configuration object ...
            configuration.addDiskStore(diskStoreConfiguration);
            manager = CacheManager.create(configuration);
        }
        // Get existing cache if there is one:
        cache = manager.getEhcache("recentlySeenUrls");
        // Otherwise, make a new one:
        if (cache == null) {
            LOGGER.info("Setting up default Ehcache configuration.");
            cache = new Cache(new CacheConfiguration("recentlySeenUrls",
                    maxEntriesLocalHeap)
                            .memoryStoreEvictionPolicy(
                                    MemoryStoreEvictionPolicy.LRU)
                            .eternal(false).timeToLiveSeconds(this.defaultTTL)
                            .diskExpiryThreadIntervalSeconds(0)
                            .maxEntriesLocalDisk(maxElementsOnDisk)
                            .diskPersistent(true).overflowToDisk(true));
            manager.addCache(cache);
        }
    }

    /**
     * 
     */
    protected boolean setAddWithTTL(String key, String uri, int ttl_s) {
        // Build the cache entry:
        Element element = new Element(key, uri);

        // Allow entries to expire after a while, defaults, ranges, etc,
        // surt-prefixed.
        element.setTimeToLive(ttl_s);

        // Add to the cache, if absent:
        Element added = getCache().putIfAbsent(element);
        if (added == null) {
            LOGGER.finest("Cache entry " + key + " > " + uri + " is new.");
        } else {
            LOGGER.finest("Cache entry " + key + " > " + uri
                    + " is already in the cache.");
        }

        return (added == null);
    }

    /**
     * 
     */
    protected boolean setRemove(CharSequence uri) {
        return getCache().remove(uri);
    }

    /**
     * Not that this in not a 'setter', it's the count (size) of a set.
     */
    protected long setCount() {
        if (this.isCacheAvailable()) {
            return getCache().getSize();
        } else {
            return 0;
        }
    }

    @Override
    public long requestFlush() {
        this.getCache().flush();
        return 0;
    }

    private void closeEhcache() {
        LOGGER.info("Shutting down Ehcache...");
        if (this.cache != null) {
            this.cache.flush();
        }
        if (this.manager != null
                && this.manager.getStatus().equals(Status.STATUS_ALIVE)) {
            this.manager.shutdown();
        }
    }

    @Override
    public void close() {
        this.closeEhcache();
        super.close();
    }

    @Override
    protected void finalize() throws Throwable {
        this.closeEhcache();
        super.finalize();
    }

    @Override
    public void start() {
        super.start();
        LOGGER.info("Called start()");
        this.setupCache();
    }

    @Override
    public void stop() {
        super.stop();
        LOGGER.info("Called stop()");
        this.closeEhcache();
    }

    @Override
    public boolean isRunning() {
        LOGGER.info("Called isRunning()");
        if (this.manager != null) {
            return this.manager.getStatus().equals(Status.STATUS_ALIVE);
        }
        return false;
    }

}
