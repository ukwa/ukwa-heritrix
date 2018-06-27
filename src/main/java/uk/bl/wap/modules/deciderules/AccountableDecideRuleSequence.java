/**
 * 
 */
package uk.bl.wap.modules.deciderules;

import org.archive.modules.CrawlURI;
import org.archive.modules.deciderules.DecideResult;
import org.archive.modules.deciderules.DecideRule;
import org.archive.modules.deciderules.DecideRuleSequence;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class AccountableDecideRuleSequence extends DecideRuleSequence {

    /**
     * 
     */
    private static final long serialVersionUID = -2720785110902201372L;

    @Override
    protected void decisionMade(CrawlURI uri, DecideRule decisiveRule,
            int decisiveRuleNumber, DecideResult result) {
        // Do the usual logging:
        super.decisionMade(uri, decisiveRule, decisiveRuleNumber, result);
        // Also add the decision to the extra info:
        uri.getExtraInfo().put("SCOPED",
                result + " by rule #" + decisiveRuleNumber + " "
                        + decisiveRule.getClass().getSimpleName());
    }

}
