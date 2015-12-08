/**
 * 
 */
package uk.bl.wap.util;

import java.util.logging.Logger;

import org.springframework.context.Lifecycle;

import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.SetArgs;
import com.lambdaworks.redis.api.StatefulRedisConnection;
import com.lambdaworks.redis.api.sync.RedisCommands;

/**
 * 
 * Redis-backed expiring URI cache.
 * 
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class RedisRecentlySeenUriUniqFilter
 extends RecentlySeenUriUniqFilter
        implements Lifecycle {

    /** */
    private static final long serialVersionUID = 7156746218148487509L;

    private static Logger LOGGER = Logger
            .getLogger(RedisRecentlySeenUriUniqFilter.class.getName());

    private String redisEndpoint = "redis://redis:6379";

    private StatefulRedisConnection<String, String> connection;

    private RedisClient redisClient;

    private RedisCommands<String, String> syncCommands;

    public RedisRecentlySeenUriUniqFilter() {
        super();
    }

    /**
     * @return the redisEndpoint
     */
    public String getRedisEndpoint() {
        return redisEndpoint;
    }

    /**
     * @param redisEndpoint
     *            the redisEndpoint to set
     */
    public void setRedisEndpoint(String redisEndpoint) {
        this.redisEndpoint = redisEndpoint;
    }

    /**
     * Initializer.
     */
    @Override
    public void afterPropertiesSet() {
        super.afterPropertiesSet();
    }

    @Override
    public void start() {
        redisClient = RedisClient.create(redisEndpoint);
        connection = redisClient
                .connect();
        syncCommands = connection.sync();

        System.out.println("Connected to Redis");
    }

    @Override
    public void stop() {
        connection.close();
        redisClient.shutdown();
    }

    @Override
    public boolean isRunning() {
        if (this.connection != null) {
            return this.connection.isOpen();
        }
        return false;
    }

    /**
     * 
     */
    protected boolean setAdd(CharSequence uri) {
        // Allow entries to expire after a while, defaults, ranges, etc,
        // surt-prefixed.
        long ttls = getTTLForUrl(uri.toString());

        // Add to the cache, if absent:
        String key = uri.toString();
        String value = uri.toString();
        SetArgs setArgs = SetArgs.Builder.nx().ex(ttls);
        // Talk to redis:
        String result = syncCommands.set(key, value, setArgs);
        // Check result:
        if (result != null) {
            LOGGER.finest("Cache entry " + uri + " is new.");
        } else {
            LOGGER.finest("Cache entry " + uri + " is already in the cache.");
        }

        return (result != null);
    }

    @Override
    protected boolean setRemove(CharSequence key) {
        long removed = syncCommands.del(key.toString());
        return (removed > 0);
    }

    @Override
    protected long setCount() {
        String result = syncCommands.info("keyspace");
        LOGGER.info("Got: " + result);
        // dbXXX: keys=XXX,expires=XXX
        // db0:keys=16224270,expires=16224270,avg_ttl=2149615
        return 0;
    }

}
