/**
 * 
 */
package uk.bl.wap.modules.uriuniqfilters;

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
 * Recently Seen URI filter based on EhCache
 * 
 * Should work and resume reliably if shutdown is normal. However, the
 * 'Enterprise' variant is needed for robust recovery options.
 * 
 * (at the moment, just getting it to reload the existing cache is proving
 * difficult)
 * 
 * It seems pointers/references are kept in RAM. Perhaps Ehcache is optimised
 * for the case where the values are large? Whatever the cause, I am finding the
 * RAM usage creeps up, with 55 million entries (after GC) when the crawl has
 * seen about that many URLs (c. 60 million).
 * 
 * Possibly, maxBytesLocalHeap might work better?
 * 
 * Loading a heap dump into Eclipse MAT and computing the Dominator Tree showed
 * Ehcache was consuming too much RAM.
 * 
 * @see http://help.eclipse.org/mars/index.jsp?topic=%2Forg.eclipse.mat.ui.help%
 *      2Freference%2Ffindingmemoryleak.html
 * @see http://help.eclipse.org/mars/index.jsp?topic=%2Forg.eclipse.mat.ui.help%
 *      2Fconcepts%2Fdominatortree.html
 * 
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

    private int maxGBLocalHeap = 10;

    private int maxElementsOnDisk = 0;

    private long sizeCounter = 0;

    private long flushFrequency = 2;

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
            LOGGER.info("Creating Ehcache manager with cacheStorePath: "
                    + cacheStorePath);
            Configuration configuration = new Configuration();
            configuration.addDiskStore(
                    new DiskStoreConfiguration().path(cacheStorePath));
            manager = new CacheManager(configuration);
            LOGGER.info("Got manager: " + manager.getActiveConfigurationText());
        }
        // List known caches:
        for (String name : manager.getCacheNames()) {
            LOGGER.info("Manager with path " + manager.getDiskStorePathManager()
                    + " knows a cache named: " + name);
        }
        // Get existing cache if there is one:
        String cacheName = "recentlyseenurls";
        cache = manager.getEhcache(cacheName);
        // Otherwise, make a new one:
        if (cache == null) {
            LOGGER.info("Setting up new, default Ehcache configuration.");
            cache = new Cache(
                    new CacheConfiguration().name(cacheName)
                            .memoryStoreEvictionPolicy(
                                    MemoryStoreEvictionPolicy.LRU)
                            .eternal(false).timeToLiveSeconds(this.defaultTTL)
                            .diskExpiryThreadIntervalSeconds(60 * 10)
                            .maxEntriesLocalDisk(maxElementsOnDisk)
                            // .maxBytesLocalHeap(maxGBLocalHeap,
                            // MemoryUnit.GIGABYTES)
                            .maxEntriesLocalHeap(maxEntriesLocalHeap)
                            .overflowToDisk(true)
                            .diskPersistent(true));
            manager.addCache(cache);
        } else {
            this.sizeCounter = cache.getSize();
        }
    }

    /**
     * 
     */
    public boolean setAddWithTTL(String key, String uri, int ttl_s) {
        // Build the cache entry:
        Element element = new Element(key, uri);

        // Allow entries to expire after a while, defaults, ranges, etc,
        // surt-prefixed.
        element.setTimeToLive(ttl_s);

        // Add to the cache, if absent:
        Element added = getCache().putIfAbsent(element);
        if (added == null) {
            LOGGER.finest("Cache entry " + key + " > " + uri + " is new.");
            this.sizeCounter++;
        } else {
            LOGGER.finest("Cache entry " + key + " > " + uri
                    + " is already in the cache.");
        }

        // Periodically flush (to control RAM usage):
        if (this.sizeCounter % flushFrequency == 0) {
            LOGGER.fine("Flushing URI cache...");
            this.cache.flush();
            LOGGER.fine("Flushed URI cache.");
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
        return this.sizeCounter;
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
