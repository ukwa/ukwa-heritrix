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
* 2.6.1:
    * Modify disposition processor so robots.txt cache does not get trashed when robots.txt get discovered outside of pre-requisites and ruled out-of-scope.
    * Update to OutbackCDX 0.5.1 requirement, taking out hack needed to cope with URLs with * in (see https://github.com/nla/outbackcdx/issues/14)
* 2.6.0:
    * Sitemap extraction and simple integration with re-crawl mechanism.

* 2.1.0:
    * Recently Seen functionality moved to a DecideRule, allowing us to use Heritrix's `recheckScope` feature to prevent recrawling of URLs that have been crawled since the original request was enqueued.
    * The OutbackCDXRecentlySeenDecideRule implementation also stores the last hash, so the `OutbackCDXPersistLoadProcessor` is no longer needed.
* 2.0.0:
    * Switched to Recently Seen unique URI filter, backed by OutbackCDX.
