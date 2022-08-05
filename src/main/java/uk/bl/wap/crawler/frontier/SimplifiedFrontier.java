/**
 * 
 */
package uk.bl.wap.crawler.frontier;

import org.archive.modules.CrawlURI;

/**
 * Idea here is to allow various more easily plugable Frontier implementations.
 * 
 * This interface represents a simplified Frontier that can be used to wrap various 
 * frontier implementations without getting too entangled with Heritrix's internal 
 * workings.
 * 
 * The (TBA) SimplifiedFrontierAdaptor class builds on Heritrix's AbstractFrontier and 
 * performs the necessary work to integrate a SimplifiedFrontier into Heritrix itself.
 * 
 * Currently shares the CrawlURI class with core Heritrix.
 * 
 * @author anj
 *
 */
public interface SimplifiedFrontier {

	/**
	 * Called after Spring initialisation, as the crawl is being launched.
	 * 
	 * Use this to set up resources like files, connections, etc.
	 */
	void start();

	/**
	 * Called on job termination.
	 * 
	 * Use to cleanly shut down resources.
	 */
	void stop();

	/**
	 * Use to check if the Frontier has started up fine and it connected/running.
	 * 
	 * @return
	 */
	boolean isRunning();

	/**
	 * Get the next URL to be crawled. 
	 * 
	 * It is up to the implementation to ensure there is only one URL from each queue/host in flight at any given time.
	 * 
	 * Should block until a URL is ready.
	 * 
	 * Should return null if there are no URLs waiting.
	 * 
	 * @return A CrawlURI to be fetched.
	 */
	CrawlURI next();

	/**
	 * Enqueue this URL.
	 * 
	 * @param curi the CrawlURI to crawl.
	 * @return True if this URL is new to the Frontier.
	 */
	boolean enqueue(String q, CrawlURI curi);

	/**
	 * De-queue this URL.
	 * 
	 * This URL has been crawled unde the given queue, and is considered 'finished'.
	 * It can therefore be removed from the queue.
	 * 
	 * @param q
	 * @param uri
	 */
	void dequeue(String q, String uri);

	/**
	 * The SimplifiedFrontier implementation is responsible for performing 
	 * whatever crawl delay Heritrix determines is appropriate.
	 * 
	 * This call should set the delay (UNIX epoch time) after which the next
	 * URL can be released from the given queue.
	 * 
	 * This also means the Heritrix ToeThread has 'released' the queue, i.e. 
	 * this is done after a URL is finished.
	 * 
	 * @param q
	 * @param fetchTime
	 */
	void delayQueue(String q, long fetchTime);

	/**
	 * Indicate that this queue is considered retired, and no more URLs should be emitted from it.
	 * 
	 * @param q
	 */
	void retireQueue(String q);

	/**
	 * Counter for reporting.
	 * 
	 * @return
	 */
	long getTotalQueues();

	/**
	 * Counter for reporting.
	 * 
	 * @return
	 */
	long getActiveQueues();

	/**
	 * Counter for reporting.
	 * 
	 * @return
	 */
	long getScheduledQueues();

	/**
	 * Counter for reporting.
	 * 
	 * @return
	 */
	long getRetiredQueues();

	/**
	 * Counter for reporting.
	 * 
	 * @return
	 */
	long getExhaustedQueues();

}
