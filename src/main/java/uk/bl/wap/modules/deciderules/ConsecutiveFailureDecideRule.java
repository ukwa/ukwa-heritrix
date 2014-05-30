package uk.bl.wap.modules.deciderules;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.archive.modules.CrawlURI;
import org.archive.modules.deciderules.DecideResult;
import org.archive.modules.deciderules.PredicatedDecideRule;

/**
 * DecideRule which, if a URI is a non-success (i.e. the HTTP status code is 401
 * or higher) and the same is true of its referrer, REJECTs said URI.
 * 
 * @author rcoram
 * 
 */

public class ConsecutiveFailureDecideRule extends PredicatedDecideRule {
    private static final long serialVersionUID = 2029296473953425876L;
    private static final Logger LOGGER = Logger
	    .getLogger(ConsecutiveFailureDecideRule.class.getName());

    {
	setDecision(DecideResult.REJECT);
    }

    @Override
    public DecideResult onlyDecision(CrawlURI uri) {
	return this.getDecision();
    }

    @Override
    protected boolean evaluate(CrawlURI curi) {
	boolean result = false;
	try {
	    result = ((curi.getFetchStatus() >= 400)
		    && (curi.getFullVia() != null) && (curi.getFullVia()
		    .getFetchStatus() >= 400));
	} catch (Exception e) {
	    LOGGER.log(Level.WARNING, curi.getURI(), e);
	    curi.getNonFatalFailures().add(e);
	}
	return result;
    }
}
