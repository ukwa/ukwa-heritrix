/**
 * 
 */
package uk.bl.wap.util;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.archive.crawler.framework.CrawlController;
import org.archive.crawler.reporting.StatisticsTracker;
import org.archive.crawler.util.CrawledBytesHistotable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.Lifecycle;

import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class MetricsExporterBean implements Lifecycle {

    private static final Logger LOGGER = Logger
            .getLogger(MetricsExporterBean.class.getName());

    private CrawlJobMetricsThread cjmt = null;

    private HTTPServer server;

    private boolean isRunning = false;

    private int metricsPort = 9118;

    public int getMetricsPort() {
        return metricsPort;
    }

    public void setMetricsPort(int metricsPort) {
        this.metricsPort = metricsPort;
    }

    protected CrawlController controller;

    public CrawlController getCrawlController() {
        return this.controller;
    }

    @Autowired
    public void setCrawlController(CrawlController controller) {
        this.controller = controller;
    }

    @Override
    public void start() {
        // Set up the default exports:
        DefaultExports.initialize();

        // Spin up a HTTP server to make the metrics available:
        try {
            server = new HTTPServer(metricsPort);
            isRunning = true;
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Problem when starting metrics endpoint.",
                    e);
        }

        // Now start the updater:
        this.cjmt = new CrawlJobMetricsThread(5);
        this.cjmt.start();
    }

    @Override
    public void stop() {
        if( server != null ) {
            server.stop();
            server = null;
        }
        if (cjmt != null) {
            cjmt.keepRunning.set(false);
        }
    }

    @Override
    public boolean isRunning() {
        return isRunning;
    }

    // ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
    // Here we define the metrics:
    // ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

    private static final Gauge m_up = Gauge.build()
            .name("heritrix3_crawl_job_state_at")
            .labelNames("crawl_host", "jobname", "state")
            .help("Heritrix3 job state timestamp.").register();

    private static final Gauge m_durations = Gauge.build()
            .name("heritrix3_crawl_job_duration_seconds")
            .labelNames("crawl_host", "jobname", "kind")
            .help("Total duration counters, in seconds.").register();

    private static final Gauge m_uris = Gauge.build()
            .name("heritrix3_crawl_job_uris_total")
            .labelNames("crawl_host", "jobname", "kind")
            .help("Total URL counters, by kind.")
            .register();

    private static final Gauge m_bytes = Gauge.build()
            .name("heritrix3_crawl_job_bytes_total")
            .labelNames("crawl_host", "jobname", "kind")
            .help("Total downloaded byte counters, by kind.").register();

    private static final Gauge m_threads = Gauge.build()
            .name("heritrix3_crawl_job_threads_total")
            .labelNames("crawl_host", "jobname", "kind")
            .help("Total thread counters, by kind.").register();

    private static final Gauge m_queues = Gauge.build()
            .name("heritrix3_crawl_job_queues_total")
            .labelNames("crawl_host", "jobname", "kind")
            .help("Total queue counters, by kind.").register();

    private static final Gauge m_queue_depth = Gauge.build()
            .name("heritrix3_crawl_job_queue_depth")
            .labelNames("crawl_host", "jobname", "kind")
            .help("Notable queue depths, by kind.").register();

    // ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
    // This thread collects the metrics:
    // ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

    /**
     * This thread works a bit like CrawlJobModel and CrawlStatSnapshot to
     * extract job stats of interest.
     * 
     * @author Andrew Jackson <Andrew.Jackson@bl.uk>
     *
     */
    public class CrawlJobMetricsThread extends Thread {

        private final String host;

        private final String jobName;

        private final int sampleInterval;

        private AtomicBoolean keepRunning = new AtomicBoolean(true);

        /**
         * 
         * @param sampleInterval
         *            in seconds
         */
        public CrawlJobMetricsThread(int sampleInterval) {
            this.setDaemon(true);
            this.sampleInterval = sampleInterval;
            this.host = getHostName();
            this.jobName = controller.getMetadata().getJobName();
        }

        public void run() {
            int sleepInterval = sampleInterval;
            while (keepRunning.get()) {
                try {
                    // Stats source:
                    StatisticsTracker stats = controller.getStatisticsTracker();

                    // General job state:
                    m_up.labels(host, jobName, getCrawlState())
                            .setToCurrentTime();

                    // Durations:
                    m_durations.labels(host, jobName, "total")
                            .set(stats.getCrawlDuration() / 1000.0);
                    m_durations.labels(host, jobName, "elapsed")
                            .set(stats.getCrawlElapsedTime() / 1000.0);

                    // Bytes totals:
                    CrawledBytesHistotable c_bytes = stats.getCrawledBytes();
                    m_bytes.labels(host, jobName, "total")
                            .set(c_bytes.getTotalBytes());

                    m_bytes.labels(host, jobName, "novel")
                            .set(c_bytes.get(CrawledBytesHistotable.NOVEL));

                    m_bytes.labels(host, jobName, "warc-novel").set(c_bytes.get(
                            CrawledBytesHistotable.WARC_NOVEL_CONTENT_BYTES));

                    m_bytes.labels(host, jobName, "deduplicated")
                            .set(c_bytes.get(CrawledBytesHistotable.DUPLICATE));

                    // URI totals:
                    m_uris.labels(host, jobName, "downloaded")
                            .set(c_bytes.getTotalUrls());

                    m_uris.labels(host, jobName, "novel").set(
                            c_bytes.get(CrawledBytesHistotable.NOVELCOUNT));

                    m_uris.labels(host, jobName, "warc-novel").set(c_bytes
                            .get(CrawledBytesHistotable.WARC_NOVEL_URLS));

                    m_uris.labels(host, jobName, "deduplicated").set(
                            c_bytes.get(CrawledBytesHistotable.DUPLICATECOUNT));

                    m_uris.labels(host, jobName, "discovered")
                            .set(controller.getFrontier().discoveredUriCount());

                    m_uris.labels(host, jobName, "finished")
                            .set(controller.getFrontier().finishedUriCount());

                    m_uris.labels(host, jobName, "queued")
                            .set(controller.getFrontier().queuedUriCount());

                    m_uris.labels(host, jobName, "future")
                            .set(controller.getFrontier().futureUriCount());

                    m_uris.labels(host, jobName, "failed_fetch")
                            .set(controller.getFrontier().failedFetchCount());

                    m_uris.labels(host, jobName, "disregarded").set(
                            controller.getFrontier().disregardedUriCount());

                    // Queue totals:
                    Map<String, Object> c_q = controller.getFrontier()
                            .shortReportMap();
                    if (c_q != null) {
                        m_queues.clear(); // Otherwise 'gone' entries get left
                                          // behind

                        m_queues.labels(host, jobName, "total")
                                .set((int) c_q.get("totalQueues"));

                        m_queues.labels(host, jobName, "in-process")
                                .set((int) c_q.get("inProcessQueues"));

                        m_queues.labels(host, jobName, "ready")
                                .set((int) c_q.get("readyQueues"));

                        m_queues.labels(host, jobName, "snoozed")
                                .set((int) c_q.get("snoozedQueues"));

                        m_queues.labels(host, jobName, "active")
                                .set((int) c_q.get("activeQueues"));

                        m_queues.labels(host, jobName, "inactive")
                                .set((int) c_q.get("inactiveQueues"));

                        m_queues.labels(host, jobName, "ineligible")
                                .set((int) c_q.get("ineligibleQueues"));

                        m_queues.labels(host, jobName, "retired")
                                .set((int) c_q.get("retiredQueues"));

                        m_queues.labels(host, jobName, "exhausted")
                                .set((int) c_q.get("exhaustedQueues"));
                    }

                    // Queue depths:
                    m_queue_depth.labels(host, jobName, "deepest")
                            .set(controller.getFrontier().deepestUri());

                    m_queue_depth.labels(host, jobName, "average")
                            .set(controller.getFrontier().averageDepth());

                    m_queue_depth.labels(host, jobName, "congestion_ratio")
                            .set(controller.getFrontier().congestionRatio());

                    // Thread totals:
                    m_threads.clear(); // Otherwise 'gone' entries get left
                                       // behind
                    m_threads.labels(host, jobName, "total")
                            .set(controller.getToeCount());

                    m_threads.labels(host, jobName, "active")
                            .set(controller.getActiveToeCount());

                    // Threads by step and processor:
                    Map<String, Object> c_t = controller
                            .getToeThreadReportShortData();
                    if (c_t != null) {
                        @SuppressWarnings("unchecked")
                        LinkedList<String> sortedSteps = (LinkedList<String>) c_t
                                .get("steps");
                        for (String step : sortedSteps) {
                            String[] parts = step.split(" ", 2);
                            Float count = Float.parseFloat(parts[0]);
                            String name = "step-" + parts[1].toLowerCase();
                            m_threads.labels(host, jobName, name).set(count);

                        }
                        @SuppressWarnings("unchecked")
                        LinkedList<String> sortedProcesses = (LinkedList<String>) c_t
                                .get("processors");
                        for (String proc : sortedProcesses) {
                            String[] parts = proc.split(" ", 2);
                            Float count = Float.parseFloat(parts[0]);
                            String name = "proc-" + parts[1].toLowerCase();
                            m_threads.labels(host, jobName, name).set(count);

                        }
                    }

                    // And sleep...
                    Thread.sleep(sleepInterval * 1000);
                } catch (InterruptedException e) {
                    LOGGER.log(Level.SEVERE,
                            "Exception when sleeping before sampling crawl metrics!",
                            e);
                    e.printStackTrace();
                }
            }
        }

        public void shutdown() {
            this.keepRunning.set(false);
        }
    }

    private static String getHostName() {
        try {
            return InetAddress.getLocalHost().getCanonicalHostName();
        } catch (UnknownHostException e) {
            LOGGER.log(Level.SEVERE, "Could not get local host name.", e);
            return "localhost";
        }
    }

    private String getCrawlState() {
        if (controller.hasStarted()) {
            return ((CrawlController.State) controller.getState()).name();
        } else {
            return "UNKNOWN";
        }
    }

}
