package uk.bl.wap.modules.extractor;

import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.archive.io.ReplayCharSequence;
import org.archive.modules.CrawlURI;
import org.archive.modules.extractor.ContentExtractor;
import org.archive.modules.extractor.Hop;
import org.archive.modules.extractor.LinkContext;
import org.archive.net.UURIFactory;

/**
 * Extractor specifically written for http://www.pilot-project.co.uk/ to parse
 * the CDATA section of each '.player' file.
 * 
 * @author rcoram
 * @param uriPattern
 *            Regex matching URIs to which this Extractor is applied.
 * @param matchPattern
 *            Regex matching Links to be extracted.
 * @param group
 *            The Regex group of the matchPattern which corresponds to the
 *            extracted Link (default: 1).
 */

public class ExtractorPattern extends ContentExtractor {

    private static final long serialVersionUID = -3296353744494407999L;
    private static Logger logger = Logger.getLogger(ExtractorPattern.class
	    .getName());

    {
	this.setUriPattern("https?://.+\\.player$");
    }

    public String getUriPattern() {
	return (String) kp.get("uriPattern");
    }

    public void setUriPattern(String uriPattern) {
	kp.put("uriPattern", Pattern.compile(uriPattern));
    }

    {
	this.setMatchPattern("\"(/tracks/[^\"]+\\.mp3)\"");
    }

    public String getMatchPattern() {
	return (String) kp.get("uriPattern");
    }

    public void setMatchPattern(String matchPattern) {
	kp.put("matchPattern", Pattern.compile(matchPattern, Pattern.MULTILINE));
    }

    {
	this.setGroup("1");
    }

    public int getGroup() {
	return (Integer) kp.get("group");
    }

    public void setGroup(String group) {
	kp.put("group", Integer.parseInt(group));
    }

    @Override
    protected boolean innerExtract(CrawlURI uri) {
	try {
	    ReplayCharSequence replay = uri.getRecorder()
		    .getContentReplayCharSequence();
	    Pattern matchPattern = (Pattern) kp.get("matchPattern");
	    Matcher matcher = matchPattern.matcher(replay.toString());
	    while (matcher.find()) {
		uri.createCrawlURI(
			UURIFactory.getInstance(matcher.group(this.getGroup())),
			LinkContext.SPECULATIVE_MISC, Hop.SPECULATIVE);
	    }
	} catch (Exception e) {
	    logger.warning(e.getMessage());
	}
	return false;
    }

    @Override
    protected boolean shouldExtract(CrawlURI uri) {
	Pattern uriPattern = (Pattern) kp.get("uriPattern");
	return uriPattern.matcher(uri.getURI()).matches();
    }

}
