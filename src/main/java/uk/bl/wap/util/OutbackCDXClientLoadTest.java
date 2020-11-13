/**
 * 
 */
package uk.bl.wap.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

/**
 * A multi-threaded load test to understand contention issues.
 * 
 * 
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class OutbackCDXClientLoadTest {

    // Some results running against an undockerised OutbackCDX on the Mac
    // laptop.
    //
    // Using a pooling client connection manager:
    // 10 751 75.1 0 1000
    // 50 596 11.92 0 5000
    // 100 947 9.47 0 10000
    // 250 1694 6.776 0 25000
    // 500 3103 6.206 0 50000
    // 1000 7581 7.581 0 100000
    //
    // Using ThreadLocal Client w/ BasicHttpClientConnectionManager:
    //
    // 10 1232 123.2 0 1000
    // 50 1086 21.72 0 5000
    // 100 1713 17.13 0 10000
    // 250 2391 9.564 0 25000
    // 500 5496 10.992 0 50000
    // 1000 8508 8.508 0 100000

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {
        OutbackCDXClientLoadTest olt = new OutbackCDXClientLoadTest();
        olt.checkCommand();
        int workload = 100;
        //
        olt.runWithThreads(10, workload);
        olt.runWithThreads(50, workload);
        olt.runWithThreads(100, workload);
        olt.runWithThreads(250, workload);
        olt.runWithThreads(500, workload);
        olt.runWithThreads(1000, workload);
    }

    public OutbackCDXClientLoadTest() {
    }

    private String[] urls = new String[] { "http://acid.matkelly.com/" };

    private void checkCommand() throws IOException {
        Runtime rt = Runtime.getRuntime();
        String[] commands = { "ulimit", "-a" };
        Process proc = rt.exec(commands);

        BufferedReader stdInput = new BufferedReader(
                new InputStreamReader(proc.getInputStream()));

        BufferedReader stdError = new BufferedReader(
                new InputStreamReader(proc.getErrorStream()));

        // Read the output from the command
        String s = null;
        while ((s = stdInput.readLine()) != null) {
            System.out.println(s);
        }

        // Read any errors from the attempted command
        while ((s = stdError.readLine()) != null) {
            System.err.println(s);
        }

    }

    public void runWithThreads(int numberOfThreads, int iterations)
            throws InterruptedException {
        //
        ExecutorService service = Executors.newFixedThreadPool(numberOfThreads);
        AtomicInteger successes = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(numberOfThreads);
        //
        long start = System.currentTimeMillis();
        // LAUNCH:
        for (int i = 0; i < numberOfThreads; i++) {
            service.submit(() -> {
                OutbackCDXClient wp = new OutbackCDXClient();
                wp.setMaxConnections(numberOfThreads);
                // wp.setEndpoint("http://localhost:9080/fc"); // Faux
                // OutbackCDX service.
                wp.setEndpoint("http://localhost:9090/fc");
                wp.setSocketTimeout(30000);

                //
                // System.out.println("getDefaultMaxPerRoute: "
                // + wp.getConnectionManager().getDefaultMaxPerRoute());
                // System.out.println("getMaxTotal: "
                // + wp.getConnectionManager().getMaxTotal());

                // Repeated lookups:
                int innerSuccesses = 0;
                for (int k = 0; k < iterations; k++) {
                    try {
                        HashMap<String, Object> info = wp.getLastCrawl(urls[0]);
                        // Thread.currentThread().sleep(50);
                        if (info != null) {
                            innerSuccesses += 1;
                        } else {
                            System.out.println("OHNO");
                        }
                    } catch (Exception e) {
                        System.err.println(e.getMessage());
                        e.printStackTrace();
                    }
                }
                // Add to total:
                successes.addAndGet(innerSuccesses);
                // Shutdown the connection pool:
                // HttpClientConnectionManager cm = wp
                // .getConnectionManager();
                // if (cm != null)
                // cm.shutdown();
                // And flag completion:
                latch.countDown();
            });
        }
        // And await completion:
        latch.await();
        long timeMillis = System.currentTimeMillis() - start;
        // Shutdown:
        service.shutdown();
        service.awaitTermination(100, TimeUnit.SECONDS);
        // And print...
        System.out.println(numberOfThreads + "\t" + timeMillis + "\t"
                + (timeMillis / (1.0 * numberOfThreads)) + "\t"
                + (iterations * numberOfThreads - successes.get()) + "\t"
                + successes.get());

    }

}
