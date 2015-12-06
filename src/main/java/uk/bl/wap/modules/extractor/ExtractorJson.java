package uk.bl.wap.modules.extractor;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.httpclient.URIException;
import org.archive.modules.CrawlURI;
import org.archive.modules.extractor.ContentExtractor;
import org.archive.modules.extractor.Hop;
import org.archive.modules.extractor.LinkContext;
import org.archive.util.UriUtils;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Extracts URIs from JSON resources.
 * 
 * n.b. chokes on JSONP, e.g.
 * 
 * breakingNews({"pollPeriod":30000,"isError":false,"html":""})
 * 
 * @author rcoram
 * 
 */

public class ExtractorJson extends ContentExtractor {
    public final static String JSON_URI = "^https?://[^/]+/.+\\.json\\b.*$";
    private static final Logger LOGGER = Logger
            .getLogger(ExtractorJson.class.getName());
    private JsonFactory factory = new JsonFactory();
    private ObjectMapper mapper = new ObjectMapper(factory);

    @Override
    protected boolean innerExtract(CrawlURI curi) {
        try {
            List<String> links = new ArrayList<String>();
            JsonNode rootNode = mapper
                    .readTree(curi.getRecorder().getContentReplayInputStream());
            parse(rootNode, links);
            for (String link : links) {
                try {
                    int max = getExtractorParameters().getMaxOutlinks();
                    addRelativeToBase(curi, max, link,
                            LinkContext.SPECULATIVE_MISC, Hop.SPECULATIVE);
                } catch (URIException e) {
                    logUriError(e, curi.getUURI(), link);
                }
            }
        } catch (Exception e) {
            // Only record this as INFO, as malformed JSON is fairly common.
            LOGGER.log(Level.INFO, curi.getURI() + " : " + e.getMessage());
        }
        return false;
    }

    @Override
    protected boolean shouldExtract(CrawlURI curi) {
        String contentType = curi.getContentType();
        if (contentType != null && contentType.indexOf("json") != -1) {
            return true;
        }

        if (curi.isSuccess() && curi.toString().matches(JSON_URI)) {
            return true;
        }
        return false;
    }

    protected List<String> parse(JsonNode rootNode, List<String> links) {
        Iterator<Map.Entry<String, JsonNode>> fieldsIterator = rootNode
                .fields();
        while (fieldsIterator.hasNext()) {
            Map.Entry<String, JsonNode> field = fieldsIterator.next();
            if (field.getValue().textValue() != null
                    && UriUtils.isVeryLikelyUri(field.getValue().textValue())) {
                links.add(field.getValue().textValue());
            } else if (field.getValue().isObject()) {
                parse(field.getValue(), links);
            } else if (field.getValue().isArray()) {
                Iterator<JsonNode> i = field.getValue().elements();
                while (i.hasNext()) {
                    parse(i.next(), links);
                }
            }
        }
        return links;
    }
}
