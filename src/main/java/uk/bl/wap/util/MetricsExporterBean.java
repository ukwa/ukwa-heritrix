/**
 * 
 */
package uk.bl.wap.util;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.springframework.context.Lifecycle;

import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class MetricsExporterBean implements Lifecycle {

    private static final Logger logger = Logger
            .getLogger(MetricsExporterBean.class.getName());

    private HTTPServer server;

    private boolean isRunning = false;

    private int metricsPort = 9118;

    public int getMetricsPort() {
        return metricsPort;
    }

    public void setMetricsPort(int metricsPort) {
        this.metricsPort = metricsPort;
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
            logger.log(Level.SEVERE, "Problem when starting metrics endpoint.",
                    e);
        }
    }

    @Override
    public void stop() {
        if( server != null ) {
            server.stop();
        }
    }

    @Override
    public boolean isRunning() {
        return isRunning;
    }

}
