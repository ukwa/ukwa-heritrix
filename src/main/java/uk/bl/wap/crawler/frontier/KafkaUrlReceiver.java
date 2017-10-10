/*
 *  This file is part of the Heritrix web crawler (crawler.archive.org).
 *
 *  Licensed to the Internet Archive (IA) by one or more individual
 *  contributors.
 *
 *  The IA licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package uk.bl.wap.crawler.frontier;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.httpclient.URIException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.archive.checkpointing.Checkpoint;
import org.archive.checkpointing.Checkpointable;
import org.archive.crawler.event.CrawlStateEvent;
import org.archive.crawler.framework.CrawlController;
import org.archive.crawler.framework.Frontier.FrontierGroup;
import org.archive.crawler.postprocessor.CandidatesProcessor;
import org.archive.modules.CrawlURI;
import org.archive.modules.SchedulingConstants;
import org.archive.modules.extractor.Hop;
import org.archive.modules.extractor.LinkContext;
import org.archive.modules.net.CrawlHost;
import org.archive.modules.net.CrawlServer;
import org.archive.modules.net.ServerCache;
import org.archive.net.UURI;
import org.archive.net.UURIFactory;
import org.archive.spring.KeyedProperties;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationListener;
import org.springframework.context.Lifecycle;


/**
 * Based on
 * /heritrix-contrib/src/main/java/org/archive/crawler/frontier/AMQPUrlReceiver.java
 * 
 * Example:
 * 
 * {"parentUrl": "http://crawl-test-site:4000/crawl-test-site/", "url": "http://crawl-test-site:4000/crawl-test-site/", "parentUrlMetadata": {"pathFromSeed": "", "heritableData": {"source": "", "heritable": ["source", "heritable"]}}, "headers": {}, "hop": "", "isSeed": true, "method": "GET"}
 * @contributor anjackson
 */
public class KafkaUrlReceiver
        implements Lifecycle, ApplicationContextAware,
        ApplicationListener<CrawlStateEvent>, Checkpointable {

    @SuppressWarnings("unused")
    private static final long serialVersionUID = 2423423423894L;

    private static final Logger logger = 
            Logger.getLogger(KafkaUrlReceiver.class.getName());

    public static final String A_RECEIVED_FROM_KAFKA = "receivedFromKafka";

    protected ApplicationContext appCtx;
    public void setApplicationContext(ApplicationContext appCtx) throws BeansException {
        this.appCtx = appCtx;
    }

    protected CandidatesProcessor candidates;
    public CandidatesProcessor getCandidates() {
        return candidates;
    }
    /**
     * Received urls are run through the supplied CandidatesProcessor, which
     * checks scope and schedules the urls. By default the crawl job's normal
     * candidates processor is autowired in, but a different one can be
     * configured if special scoping rules are desired.
     */
    @Autowired
    public void setCandidates(CandidatesProcessor candidates) {
        this.candidates = candidates;
    }

    protected ServerCache serverCache;

    public ServerCache getServerCache() {
        return this.serverCache;
    }

    @Autowired
    public void setServerCache(ServerCache serverCache) {
        this.serverCache = serverCache;
    }

    protected String bootstrapServers = "localhost:9092";
    public String getBootstrapServers() {
        return this.bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    protected String groupId = "crawlers";
    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    protected String topic = "uris-tocrawl";
    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    private boolean seekToBeginning = true;

    protected boolean isRunning = false; 

    @Override
    public boolean isRunning() {
        return isRunning;
    }

    /**
     * The maximum prefetch count to use, meaning the maximum number of messages
     * to be consumed without being acknowledged. Using 'null' would specify
     * there should be no upper limit (the default).
     */
    private Integer pollTimeout = 1000;

    private transient Lock lock = new ReentrantLock(true);

    public class KafkaConsumerRunner implements Runnable {
        private final AtomicBoolean closed = new AtomicBoolean(false);
        private final KafkaConsumer<String, byte[]> consumer;

        public KafkaConsumerRunner(boolean seekToBeginning) {
            logger.info("Setting up KafkaConsumerRunner...");
            Properties props = new Properties();
            props.put("bootstrap.servers", getBootstrapServers());
            props.put("group.id", getGroupId());
            props.put("enable.auto.commit", "true");
            props.put("auto.commit.interval.ms", "1000");
            props.put("session.timeout.ms", "30000");
            props.put("max.poll.records", "1000");
            props.put("auto.offset.reset", "earliest");
            props.put("key.deserializer", StringDeserializer.class.getName());
            props.put("value.deserializer",
                    ByteArrayDeserializer.class.getName());
            consumer = new KafkaConsumer<String, byte[]>(props);
            // Subscribe:
            consumer.subscribe(Arrays.asList(getTopic()));
            // Rewind if requested:
            if (seekToBeginning) {
                logger.warning("Rewinding to the beginning of the " + getTopic()
                        + " URL queue.");
                seekToBeginning();
            } else {
                logger.info("Resuming consumption of the " + getTopic()
                        + " URL queue.");
            }

        }

        public void run() {
            logger.info("Running KafkaConsumer... :: bootstrapServers = "
                    + getBootstrapServers());
            logger.info("Running KafkaConsumer... :: topic = " + getTopic());
            logger.info(
                    "Running KafkaConsumer... :: group_id = " + getGroupId());
            // And now poll for records:
            while (!closed.get()) {
                ConsumerRecords<String, byte[]> records = consumer
                        .poll(pollTimeout);
                // Handle new records
                for (ConsumerRecord<String, byte[]> record : records) {
                    try {
                        String decodedBody = new String(record.value(),
                                "UTF-8");
                        logger.finer(
                                "Processing crawl request: " + decodedBody);
                        JSONObject jo = new JSONObject(decodedBody);

                        if ("GET".equals(jo.getString("method"))) {
                            try {
                                CrawlURI curi = makeCrawlUri(jo);
                                KeyedProperties.clearAllOverrideContexts();
                                if (curi.isSeed()) {
                                    candidates.getSeeds().addSeed(curi);
                                    // Also clear any quotas if a seed is marked
                                    // as forced:
                                    if (curi.forceFetch()) {
                                        logger.info(
                                                "Clearing down quota stats for "
                                                        + curi);
                                        // Group stats:
                                        FrontierGroup group = candidates
                                                .getFrontier().getGroup(curi);
                                        group.getSubstats().clear();
                                        group.makeDirty();
                                        // By server:
                                        final CrawlServer server = serverCache
                                                .getServerFor(curi.getUURI());
                                        server.getSubstats().clear();
                                        server.makeDirty();
                                        // And by host:
                                        final CrawlHost host = serverCache
                                                .getHostFor(curi.getUURI());
                                        host.getSubstats().clear();
                                        host.makeDirty();
                                    }
                                } else {
                                    candidates.runCandidateChain(curi, null);
                                }
                            } catch (URIException e) {
                                logger.log(Level.WARNING,
                                        "problem creating CrawlURI from json received via Kafka "
                                                + decodedBody,
                                        e);
                            } catch (JSONException e) {
                                logger.log(Level.SEVERE,
                                        "problem creating CrawlURI from json received via Kafka "
                                                + decodedBody,
                                        e);
                            } catch (Exception e) {
                                logger.log(Level.SEVERE,
                                        "Unanticipated problem creating CrawlURI from json received via Kafka "
                                                + decodedBody,
                                        e);
                            }

                        } else {
                            logger.info(
                                    "ignoring url with method other than GET - "
                                            + decodedBody);
                        }
                    } catch (Exception e) {
                        logger.log(Level.SEVERE,
                                "problem creating JSON from String received via Kafka "
                                        + record.value(),
                                e);
                    }

                }

            }
        }

        /**
         * This can be used to seek to the start of the Kafka feed after
         * subscribing:
         */
        public void seekToBeginning() {
            if (consumer.subscription().size() > 0) {
                // Ensure partitions have been assigned by running a .poll():
                consumer.poll(0);
                // Reset to the start:
                consumer.seekToBeginning(consumer.assignment());
            }
        }

        // Shutdown hook which can be called from a separate thread
        public void shutdown() {
            closed.set(true);
            // This forced .poll to exit with a
            // org.apache.kafka.common.errors.WakeupException
            // Not sure that's really needed.
            // consumer.wakeup();
        }
    }

    transient private KafkaConsumerRunner kafkaConsumer;
    transient private ThreadGroup kafkaProducerThreads;

    @Override
    public void start() {
    }

    @Override
    public void stop() {
        lock.lock();
        try {
            logger.info("Shutting down on STOP event...");
            if (isRunning) {
                kafkaConsumer.shutdown();
                isRunning = false;
            }
        } finally {
            lock.unlock();
        }
    }

    protected static final Set<String> REQUEST_HEADER_BLACKLIST = new HashSet<String>(Arrays.asList(
            "accept-encoding", "upgrade-insecure-requests", "host", "connection"));


    // {
    // "headers": {
    // "Referer": "https://archive.org/",
    // "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML,
    // like Gecko) Ubuntu Chromium/32.0.1700.102 Chrome/32.0.1700.102
    // Safari/537.36",
    // "Accept": "image/webp,*/*;q=0.8"
    // },
    // "url":
    // "https://analytics.archive.org/0.gif?server_ms=256&server_name=www19.us.archive.org&service=ao&loadtime=358&timediff=-8&locale=en-US&referrer=-&version=2&count=9",
    // "method": "GET"
    // }
    protected CrawlURI makeCrawlUri(JSONObject jo)
            throws URIException, JSONException {

        UURI uuri = UURIFactory.getInstance(jo.getString("url"));
        UURI via = UURIFactory.getInstance(jo.getString("parentUrl"));

        JSONObject parentUrlMetadata = jo.getJSONObject("parentUrlMetadata");
        String parentHopPath = parentUrlMetadata.getString("pathFromSeed");
        String hop = jo.optString("hop", Hop.INFERRED.getHopString());
        String hopPath = parentHopPath + hop;

        CrawlURI curi = new CrawlURI(uuri, hopPath, via,
                LinkContext.INFERRED_MISC);

        populateHeritableMetadata(curi, parentUrlMetadata);

        // set the http headers from the Kafka message:
        if (jo.has("headers")) {
            JSONObject joHeaders = jo.getJSONObject("headers");
            Map<String, String> customHttpRequestHeaders = new HashMap<String, String>();
            for (Object key : joHeaders.keySet()) {
                String k = key.toString();
                if (!k.startsWith(":")
                        && !REQUEST_HEADER_BLACKLIST.contains(k)) {
                    customHttpRequestHeaders.put(k,
                            joHeaders.getString(key.toString()));
                }
            }
            curi.getData().put("customHttpRequestHeaders",
                    customHttpRequestHeaders);
        }

        /*
         * Crawl job must be configured to use HighestUriQueuePrecedencePolicy
         * to ensure these high priority urls really get crawled ahead of
         * others. See
         * https://webarchive.jira.com/wiki/display/Heritrix/Precedence+
         * Feature+Notes
         */
        if (Hop.INFERRED.getHopString().equals(curi.getLastHop())) {
            curi.setSchedulingDirective(SchedulingConstants.HIGH);
            curi.setPrecedence(1);
        }

        curi.setForceFetch(jo.optBoolean("forceFetch"));
        curi.setSeed(jo.optBoolean("isSeed"));

        curi.getAnnotations().add(A_RECEIVED_FROM_KAFKA);

        return curi;
    }

    // set the heritable data from the parent url, passed back to us via Kafka
    // XXX brittle, only goes one level deep, and only handles strings and
    // arrays, the latter of which it converts to a Set.
    // 'heritableData': {'source': 'https://facebook.com/whitehouse/',
    // 'heritable': ['source', 'heritable']}
    @SuppressWarnings("unchecked")
    protected void populateHeritableMetadata(CrawlURI curi,
            JSONObject parentUrlMetadata) {
        JSONObject heritableData = parentUrlMetadata
                .getJSONObject("heritableData");
        for (String key : (Set<String>) heritableData.keySet()) {
            Object value = heritableData.get(key);
            if (value instanceof JSONArray) {
                Set<String> valueSet = new HashSet<String>();
                JSONArray arr = ((JSONArray) value);
                for (int i = 0; i < arr.length(); i++) {
                    valueSet.add(arr.getString(i));
                }
                curi.getData().put(key, valueSet);
            } else {
                curi.getData().put(key, heritableData.get(key));
            }
        }
    }

    @Override
    public void onApplicationEvent(CrawlStateEvent event) {
        switch(event.getState()) {
        case PAUSING: case PAUSED:
            if (this.isRunning) {
                logger.info("Requesting shutdown of the KafkaURLReceiver...");
                this.kafkaConsumer.shutdown();
                this.isRunning = false;
            }
            break;

        case RUNNING:
            if (!this.isRunning) {
                logger.info("Requesting launch of the KafkaURLReceiver...");
                kafkaConsumer = new KafkaConsumerRunner(seekToBeginning);
                kafkaProducerThreads = new ThreadGroup(
                        Thread.currentThread().getThreadGroup().getParent(),
                        "KafkaProducerThreads");
                ThreadFactory threadFactory = new ThreadFactory() {
                    public Thread newThread(Runnable r) {
                        return new Thread(kafkaProducerThreads, r);
                    }
                };
                ExecutorService executorService = Executors
                        .newFixedThreadPool(1, threadFactory);
                executorService.execute(kafkaConsumer);

                this.isRunning = true;
                // Only seek to the beginning at the start of the crawl:
                if (seekToBeginning) {
                    seekToBeginning = false;
                }
            }
            break;

        default:
        }
    }

    public static void main(String[] args) {
        KafkaUrlReceiver u = new KafkaUrlReceiver();
        u.setBootstrapServers("kafka:9092");
        u.setGroupId("test");
        u.setTopic("uris-tocrawl");
        u.start();
        u.onApplicationEvent(
                new CrawlStateEvent("", CrawlController.State.RUNNING, ""));
    }

    @Override
    public void startCheckpoint(Checkpoint checkpointInProgress) {
        // No action needed.
    }

    @Override
    public void doCheckpoint(Checkpoint checkpointInProgress)
            throws IOException {
        // No action needed.
    }

    @Override
    public void finishCheckpoint(Checkpoint checkpointInProgress) {
        // No action needed.
    }

    @Override
    public void setRecoveryCheckpoint(Checkpoint recoveryCheckpoint) {
        // It seems we should recover from a checkpoint. In that case DO NOT
        // rewind the Kafka queue.
        this.seekToBeginning = false;

    }
}
