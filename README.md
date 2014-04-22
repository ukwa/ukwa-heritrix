bl-heritrix-modules
===================

To build:

    mvn clean compile assembly:single

Modules for Heritrix 3.

* AnnotationMatchesListRegexDecideRule: DecideRule for checking against annotations.
* AsynchronousMQExtractor: publishes messages to an external queue for processing (see 'WebTools'; https://github.com/openplanets/wap.git)
* ClamdScanner: for processing in an external ClamAv daemon.
* CompressibilityDecideRule: DecideRule based on a URI's compressibility (*untested).
* DbSeedModule: module for pulling seeds from an external DB (*no longer used).
* ExtractorJson: extracts URIs from JSON-formatted data.
* ExtractorPattern: extracts URIs based on regular expressions (*written explicitly for one site; not widely used).
* IpAnnotator: annotates each URI with the IP.
* LinkLogger: posts entries to an external SOAP webservice (*no longer used).
* ViralContentProcessor: passes incoming URIs to ClamAv.
* WARCConstants, WARCViralWriterProcessor, XorInputStream: workarounds for force-writing of 'conversion' records based on XOR'd version of the original data.
