/**
 * 
 */
package uk.bl.wap.util;

import java.util.logging.Logger;

import org.springframework.context.Lifecycle;

import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnection;
import com.lambdaworks.redis.protocol.SetArgs;

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

    private int redisDB = 0;

    private RedisConnection<String, String> connection;

    private RedisClient redisClient;

    // (v4 API) private RedisCommands<String, String> syncCommands;

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
     *            the redisEndpoint to set, defaults to "redis://redis:6379"
     */
    public void setRedisEndpoint(String redisEndpoint) {
        this.redisEndpoint = redisEndpoint;
    }

    /**
     * @return the DB number
     */
    public int getDB() {
        return redisDB;
    }

    /**
     * @param DB
     *            the DB number to use, defaults to 0
     */
    public void setDB(int DB) {
        this.redisDB = DB;
    }

    @Override
    public void start() {
        redisClient = RedisClient.create(redisEndpoint);
        connection = redisClient
                .connect();

        // Select the database to use:
        connection.select(redisDB);

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
    protected boolean setAddWithTTL(String key, String uri, int ttl_s) {
        // Add to the cache, if absent:
        SetArgs setArgs = SetArgs.Builder.nx().ex(ttl_s);
        // Talk to redis:
        String result = connection.set(key, uri, setArgs);
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
        long removed = connection.del(key.toString());
        return (removed > 0);
    }

    @Override
    protected long setCount() {
        String result = connection.info("keyspace");
        LOGGER.info("Got: " + result);
        // dbXXX: keys=XXX,expires=XXX
        // db0:keys=16224270,expires=16224270,avg_ttl=2149615
        return 0;
    }

}
