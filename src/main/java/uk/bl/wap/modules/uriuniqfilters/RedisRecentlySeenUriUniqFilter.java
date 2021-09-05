/**
 * 
 */
package uk.bl.wap.modules.uriuniqfilters;

import java.util.logging.Logger;

import org.springframework.context.Lifecycle;

import io.lettuce.core.RedisClient;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;


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

    private String endpoint = "redis://redis:6379";

    private int redisDB = 0;

    private RedisClient client;

    private StatefulRedisConnection<String, String> connection;
    
    private RedisCommands<String, String> commands;

    public RedisRecentlySeenUriUniqFilter() {
        super();
    }

    /**
     * @return the redisEndpoint
     */
    public String getEndpoint() {
        return endpoint;
    }

    /**
     * @param endpoint
     *            the redisEndpoint to set, defaults to "redis://redis:6379"
     */
    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
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

    /**
     * 
     */
    private void connect() {
        client = RedisClient.create(endpoint);
        connection = client.connect();
        commands = connection.sync();

        // Select the database to use:
        commands.select(redisDB);

        System.out.println("Connected to Redis");
    }

    @Override
    public void start() {
        this.connect();
    }

    @Override
    public void stop() {
        connection.close();
        client.shutdown();
    }

    @Override
    public boolean isRunning() {
        if (this.connection != null) {
            return this.connection.isOpen();
        }
        return false;
    }

    /**
     * (see https://redis.io/commands/set)
     */
    public boolean setAddWithTTL(String key, String uri, int ttl_s) {
        // Add to the cache, if absent, with the given TTL:
        SetArgs setArgs = SetArgs.Builder.nx().ex(ttl_s);
        // Talk to redis:
        String result = commands.set(key, uri, setArgs);
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
        long removed = commands.del(key.toString());
        return (removed > 0);
    }

    @Override
    protected long setCount() {
        if (connection != null && connection.isOpen()) {
            String result = commands.info("keyspace");
            LOGGER.finest("Got: " + result);
            return parseKeyspaceInfo(result);
        }
        return 0;
    }

    // e.g. dbXXX: keys=XXX,expires=XXX
    // i.e. db0:keys=16224270,expires=16224270,avg_ttl=2149615
    protected static long parseKeyspaceInfo(String result) {
        String[] parts = result.split("[=,]+");
        if (parts.length > 1) {
            return Integer.parseInt(parts[1]);
        } else {
            return 0;
        }
    }

}
