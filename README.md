bl-heritrix-modules
===================

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

