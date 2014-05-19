package uk.bl.wap.modules.deciderules;

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

    {
	setDecision(DecideResult.REJECT);
    }

    @Override
    protected boolean evaluate(CrawlURI curi) {
	return ((curi.getFetchStatus() >= 400) && (curi.getFullVia()
		.getFetchStatus() >= 400));
    }

}
