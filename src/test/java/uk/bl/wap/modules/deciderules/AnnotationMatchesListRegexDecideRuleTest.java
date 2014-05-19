package uk.bl.wap.modules.deciderules;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.httpclient.URIException;
import org.archive.modules.CrawlURI;
import org.archive.modules.deciderules.DecideResult;
import org.archive.modules.extractor.LinkContext;
import org.archive.net.UURI;
import org.archive.net.UURIFactory;
import org.junit.Assert;
import org.junit.Test;

public class AnnotationMatchesListRegexDecideRuleTest {

    @Test
    public void testInList() throws Exception {
	Pattern[] p = { Pattern.compile("^.*annotation.*$"),
		Pattern.compile("^.*empty.*$") };
	List<Pattern> patterns = new ArrayList<Pattern>(Arrays.asList(p));
	AnnotationMatchesListRegexDecideRule a = makeDecideRule(
		DecideResult.ACCEPT, patterns, false);
	CrawlURI testUri = createTestUri("http://www.bl.uk/");
	testUri.getAnnotations().add("This is an annotation.");

	Assert.assertEquals(DecideResult.NONE, a.decisionFor(testUri));
    }

    @Test
    public void testAcceptInORList() throws Exception {
	Pattern[] p = { Pattern.compile("^.*annotation.*$"),
		Pattern.compile("^.*empty.*$") };
	List<Pattern> patterns = new ArrayList<Pattern>(Arrays.asList(p));
	AnnotationMatchesListRegexDecideRule a = makeDecideRule(
		DecideResult.ACCEPT, patterns, true);
	CrawlURI testUri = createTestUri("http://www.bl.uk/");
	testUri.getAnnotations().add("This is an annotation.");

	Assert.assertEquals(DecideResult.ACCEPT, a.decisionFor(testUri));
    }

    @Test
    public void testNotInList() throws Exception {
	Pattern[] p = { Pattern.compile("^.*annotation.*$"),
		Pattern.compile("^.*empty.*$") };
	List<Pattern> patterns = new ArrayList<Pattern>(Arrays.asList(p));
	AnnotationMatchesListRegexDecideRule a = makeDecideRule(
		DecideResult.ACCEPT, patterns, false);
	CrawlURI testUri = createTestUri("http://www.bl.uk/");
	testUri.getAnnotations()
		.add("damaged ability drain air domain attack roll balance domain base save bonus.");

	Assert.assertEquals(DecideResult.NONE, a.decisionFor(testUri));
    }

    @Test
    public void testNotInORList() throws Exception {
	Pattern[] p = { Pattern.compile("^.*annotation.*$"),
		Pattern.compile("^.*empty.*$") };
	List<Pattern> patterns = new ArrayList<Pattern>(Arrays.asList(p));
	AnnotationMatchesListRegexDecideRule a = makeDecideRule(
		DecideResult.ACCEPT, patterns, true);
	CrawlURI testUri = createTestUri("http://www.bl.uk/");
	testUri.getAnnotations()
	.add("melee attack nonplayer character one-handed weapon paralyzed saving throw.");

	Assert.assertEquals(DecideResult.NONE, a.decisionFor(testUri));
    }

    @Test
    public void testEmptyAnnotations() throws Exception {
	Pattern[] p = { Pattern.compile("^.*annotation.*$"),
		Pattern.compile("^.*empty.*$") };
	List<Pattern> patterns = new ArrayList<Pattern>(Arrays.asList(p));
	AnnotationMatchesListRegexDecideRule a = makeDecideRule(
		DecideResult.ACCEPT, patterns, true);
	CrawlURI testUri = createTestUri("http://www.bl.uk/");
	testUri.getAnnotations().clear();

	Assert.assertEquals(DecideResult.NONE, a.decisionFor(testUri));
    }

    private CrawlURI createTestUri(String urlStr) throws URIException {
	UURI testUuri = UURIFactory.getInstance(urlStr);
	CrawlURI testUri = new CrawlURI(testUuri, null, null,
		LinkContext.NAVLINK_MISC);

	return testUri;
    }

    private AnnotationMatchesListRegexDecideRule makeDecideRule(
	    DecideResult decision, List<Pattern> patterns, boolean listLogicOR) {
	AnnotationMatchesListRegexDecideRule a = new AnnotationMatchesListRegexDecideRule();
	a.setRegexList(patterns);
	a.setDecision(decision);
	a.setListLogicalOr(listLogicOR);
	return a;
    }

}
