bl-heritrix-modules
===================

To test and build:

    mvn clean install assembly:single

Modules for Heritrix 3.

* AnnotationMatchesListRegexDecideRule: DecideRule for checking against annotations.
* AsynchronousMQExtractor: publishes messages to an external queue for processing (see 'WebTools'; https://github.com/openplanets/wap.git)
* ClamdScanner: for processing in an external ClamAv daemon.
* CompressibilityDecideRule: REJECTs highly-compressable (and highly incompressibl) URIs.
* ConsecutiveFailureDecideRule: REJECTs a URI if both it and its referrer's HTTP status codes are >= 400.
* ExternalGeoLookup: implementation of ExternalGeoLookupInterface for use with a ExternalGeoLocationDecideRule; uses MaxMind's GeoLite2 database.
* ExtractorJson: extracts URIs from JSON-formatted data.
* ExtractorPattern: extracts URIs based on regular expressions (*written explicitly for one site; not widely used).
* IpAnnotator: annotates each URI with the IP.
* ViralContentProcessor: passes incoming URIs to ClamAv.
* WARCViralWriterProcessor, XorInputStream: workarounds for force-writing of 'conversion' records based on XOR'd version of the original data.
