UKWA Heritrix
=============


To run a test crawl:

    $ docker-compose up

and somewhere else

    $ cat testdata/seed.json | kafka-console-producer --broker-list localhost:9092 --topic uris-to-crawl


    $ kafka-console-consumer --bootstrap-server kafka:9092 --topic uris-to-crawl --from-beginning
    
    
    $ kafka-console-consumer --bootstrap-server kafka:9092 --topic frequent-crawl-log --from-beginning
    
    
Heritrix3 Crawl Jobs
--------------------

We use [Heririx3 Sheets](https://webarchive.jira.com/wiki/spaces/Heritrix/pages/5735723/Sheets) as a configuration mechanism to allow the crawler behaviour to change based on URL SURT prefix.


Summary of Heritrix3 Modules
----------------------------

To test and build:

    mvn clean install assembly:single

Modules for Heritrix 3.3.0+

* AnnotationMatchesListRegexDecideRule: DecideRule for checking against annotations.
* AsynchronousMQExtractor: publishes messages to an external queue for processing (see '[WebTools](https://github.com/openplanets/wap.git)'; )
* ClamdScanner: for processing in an external ClamAv daemon.
* CompressibilityDecideRule: REJECTs highly-compressable (and highly incompressibl) URIs.
* ConsecutiveFailureDecideRule: REJECTs a URI if both it and its referrer's HTTP status codes are >= 400.
* CountryCodeAnnotator: adds a country-code annotation to each URI where present.
* ExternalGeoLookup: implementation of ExternalGeoLookupInterface for use with a ExternalGeoLocationDecideRule; uses MaxMind's [GeoLite2](http://dev.maxmind.com/geoip/geoip2/geolite2/) database.
* ExtractorJson: extracts URIs from JSON-formatted data.
* ExtractorPattern: extracts URIs based on regular expressions (*written explicitly for one site; not widely used).
* HashingCrawlMapper: intended as a simpler version of the HashCrawlMapper using the Hashing libraries.
* IpAnnotator: annotates each URI with the IP.
* ViralContentProcessor: passes incoming URIs to ClamAv.
* WARCViralWriterProcessor, XorInputStream: workarounds for force-writing of 'conversion' records based on XOR'd version of the original data.
* RobotsTxtSitemapExtractor: Extracts and enqueues sitemap links from robots.txt files.
* WrenderProcessor: Runs pages through a web-rendering web service rather than the usual H3 processing.


cat testdata/seed.json | kafka-console-producer --broker-list kafka:9092 --topic uris-to-crawl
kafka-console-consumer --bootstrap-server kafka:9092 --topic uris-to-crawl --from-beginning

kafka-console-consumer --bootstrap-server kafka:9092 --topic frequent-crawl-log --from-beginning



https://webarchive.jira.com/wiki/spaces/Heritrix/pages/5735014/Heritrix+3.x+API+Guide

Changes
-------
* 2.6.3:
    * Handle case where incoming non-seed URIs were not getting logged right because the inscope log builder assumes there is a source CrawlURI.
* 2.6.2:
    * Allow up to 50,000 outlinks per CrawlURI, to cope with the case of large sitemaps.
* 2.6.1:
    * Modify disposition processor so robots.txt cache does not get trashed when robots.txt get discovered outside of pre-requisites and ruled out-of-scope.
    * Update to OutbackCDX 0.5.1 requirement, taking out hack needed to cope with URLs with * in (see https://github.com/nla/outbackcdx/issues/14)
* 2.6.0:
    * Sitemap extraction and simple integration with re-crawl mechanism.
* 2.5.2:
    * Revert to skipping whole disposition chain as robots.txt cache was getting trashed by -5000 responses.
* 2.5.1:
    * Make number of WARC writers configurable (as 5 was a bottleneck under DC).
* 2.5.0:
    * Simplify shutdown logic to avoid lock files lingering.
    * Allow over-quota queues to be retired and re-awakened instead of emitting all.
    * Skip to disposition processor rather than skipping the whole chain, so the recrawl delay get set right.
* 2.4.15:
    * Fix Spring syntax problem.
* 2.4.14:
    * Attempt checkpoint during Docker shutdown.
    * Allow WebRender maxTries to be configured. 
* 2.4.13:
    * Make WebRender timeouts configurable, and use WEBRENDER as the default prefix.
    * Emit in-scope URLs into a separate Kafka topic.
* 2.4.12:
    * Only reset sheets if the sheets are being modified, to allow simple 'refresh' requests.
* 2.4.11:
    * Copy the sheets so they 'stick' even if we change the targetSheet.
* 2.4.10:
    * Ensure sheets are unchanged if unspecified.
* 2.4.9:
    * Refactor and simplify RecentlySeen code and default to ignoring forceFetch.
* 2.4.8:
    * Reset sheet definitions for recrawl delays, as launchTimestamp resolves the issue.
* 2.4.7:
    * Update to Heritrix 3.4.0-20190418 (and so avoid caching DNS failures for ever)
    * Make startup support none or new-crawl as well as resume-latest.
* 2.4.6:
    * Ensure critical tasks are handled before skipping the disposition chain.
    * Allow per-URL override of sheet-based launchTimestamp, consistently named. 
* 2.4.5:
    * Switch to generic per-launch sheet setup. i.e. 'target sheets' where any property can be set.
    * Create sheet for every target, set launchTimestamp that way.
    * Switch to hostMaxSuccessKb rather than serverMaxSuccessKb because clearing server quotas was brittle across HTTP/HTTPS.
* 2.4.4:
    * Add ability to skip the disposition chain if it's a recently-seen URI and hence out of scope.
    * Make WebRender more patient.
    * Renamed Metrics Bean so it's clear it's about Prometheus.
* 2.4.3:
    * Allow alert count to be monitored via Prometheus.
    * Log known issue with pre-0.5.1 OutbackCDX URL handling when there's an asterisk.
* 2.4.2:
    * Revert to Heritrix 3.4.0-20190207 rather than trialing 3.4.0-SNAPSHOT.
* 2.4.1:
    * Switch to clearing host quotas rather than server quotas.
    * Add DOI.org as a known URL shortener (i.e. always resolve via)
    * Restore URL shorteners list.
* 2.4.0:
    * Allow use of launch timestamp to control re-crawls. 
    * Process error outlinks (because WebRendered come back as 'error code' -5002), and make WebRendered items clear. 
    * Ensure critical pre-requisites are not blocked by the quotas, because quotas are only cleared when the seeds go through.
* 2.3.6:
    * Revert to server quota, and avoid messing with frontier groups.
* 2.3.5:
    * Use and clear host quotas instead, to avoid http/https problem. 
* 2.3.4:
    * Support NOT routing via Kafka properly.
    * Simplify sheet logic layout.
* 2.3.3:
    * Make operation of quota reset clear. 
    * Give H3 time to start up. 
* 2.3.2:
    * Use consistent envvar name for Web Render config.
    * Use newer webrender-api service in docker-compose file.
    * Slashes not allowed in WARC prefix for warcprox. Removed them.
    * Update to Java 8 on Travis
* 2.3.1:
    * Store WebRender WARCs next to the normal ones -- Didn't work, see 2.3.2.
    * Update build to Java 8 in Maven.
    * Update pre-send scoping logic to match candidates processor. 
    * Reducing logging of forgetting URLs. 
* 2.3.0:
    * Switching to a 'forgetful' URI filter, BdbUriUniqFilter plus ForgettingFrontierProcessor.
    * Switch to using an annotation for quota resets. 
    * Now running with Puppeteer as an alternative renderer 
    * Shift quota-reset into a processor.
    * Ensure sheets are applied if post-filtering. 
    * Always get embeds.
* 2.2.20:
    * Shorten recrawl periods somewhat, to attempt to avoid issues with crawls being skipped.
    * Ensure resetting quotas avoids any possible race condition. 
* 2.2.19:
    * Docker build rely on Maven to handle H3 version etc. 
    * Allow OSSRH Snapshots. 
    * Assemble Heritrix from Maven. 
    * Experiment with a snapshot build and only core repos.
* 2.2.18:
     * Fix NPW in WebRender numTries check.
* 2.2.17:
     * Update to 3.4.0-20190207 H3 build.
     * Make it possible to disable outgoing logs. 
     * Shifting to 3.4 H3 release. 
* 2.2.16:
     * Use IA SNAPSHOT binary release.
     * Update WebRenderCount annotation properly. 
* 2.2.15:
     * Switch to using the term WebRender rather than Wrender.
     * Simplify web renderer to allow H3 to handle retries properly.
     * Add an explicit client id to the Kafka client.
     * Name Prometheus metric consistently.
 * 2.2.14:
     * Apply sheets to HTTPS and HTTP. 
 * 2.2.13:
     * Try alternate quota reset logic.
     * Ensure Kafka offsets get committed properly.
     * Switch to keeping all checkpoints by default, to avoid messing with log files.
 * 2.2.12:
     * Ensure unique groupId per consumer, as required for manual partition in Kafka.
     
    

* 2.1.0:
    * Recently Seen functionality moved to a DecideRule, allowing us to use Heritrix's `recheckScope` feature to prevent recrawling of URLs that have been crawled since the original request was enqueued.
    * The OutbackCDXRecentlySeenDecideRule implementation also stores the last hash, so the `OutbackCDXPersistLoadProcessor` is no longer needed.
* 2.0.0:
    * Switched to Recently Seen unique URI filter, backed by OutbackCDX.
