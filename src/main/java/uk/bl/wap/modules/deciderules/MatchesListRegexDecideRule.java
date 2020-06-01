package uk.bl.wap.modules.deciderules;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import org.archive.modules.CrawlURI;
import org.archive.modules.deciderules.PredicatedDecideRule;
import org.archive.util.InterruptibleCharSequence;

/**
 * Rule applies configured decision to any CrawlURIs whose String URI matches
 * the supplied regexs.
 * <p>
 * The list of regular expressions can be considered logically AND or OR.
 * 
 * ANJ TEMPORARY COPY TO AVOID ISSUE WITH LAMBDA
 * https://github.com/internetarchive/heritrix3/issues/337
 *
 * @author Kristinn Sigurdsson
 *
 * @see MatchesRegexDecideRule
 */
public class MatchesListRegexDecideRule extends PredicatedDecideRule {
    private static final long serialVersionUID = 3L;
    private static final Logger logger = Logger
            .getLogger(MatchesListRegexDecideRule.class.getName());

    /**
     * The timeout for regular expression matching, in seconds. If set to 0 or
     * negative then no timeout is specified and there is no upper limit to how
     * long the matching may take. See the corresponding test class
     * MatchesListRegexDecideRuleTest for a pathological example.
     */
    {
        setTimeoutPerRegexSeconds(0L);
    }

    public long getTimeoutPerRegexSeconds() {
        return (Long) kp.get("timeout");
    }

    public void setTimeoutPerRegexSeconds(long timeoutPerRegexSeconds) {
        kp.put("timeout", timeoutPerRegexSeconds);
    }

    /**
     * The list of regular expressions to evalute against the URI.
     */
    {
        setRegexList(new ArrayList<Pattern>());
    }

    @SuppressWarnings("unchecked")
    public List<Pattern> getRegexList() {
        return (List<Pattern>) kp.get("regexList");
    }

    public void setRegexList(List<Pattern> patterns) {
        kp.put("regexList", patterns);
    }

    /**
     * True if the list of regular expression should be considered as logically
     * AND when matching. False if the list of regular expressions should be
     * considered as logically OR when matching.
     */
    {
        setListLogicalOr(true);
    }

    public boolean getListLogicalOr() {
        return (Boolean) kp.get("listLogicalOr");
    }

    public void setListLogicalOr(boolean listLogicalOr) {
        kp.put("listLogicalOr", listLogicalOr);
    }

    /**
     * Usual constructor.
     */
    public MatchesListRegexDecideRule() {
    }

    /**
     * Helper class to avoid problem with Spring and lambdas...
     * 
     * @author Andrew Jackson <Andrew.Jackson@bl.uk>
     *
     */
    static class MatcherTask implements Callable<Boolean> {

        Pattern p;
        InterruptibleCharSequence interruptible;

        MatcherTask(Pattern p, InterruptibleCharSequence interruptible) {
            this.p = p;
            this.interruptible = interruptible;
        }

        @Override
        public Boolean call() throws Exception {
            return p.matcher(interruptible).matches();
        }

    }

    /**
     * Evaluate whether given object's string version matches configured regexes
     */
    @Override
    protected boolean evaluate(CrawlURI uri) {
        List<Pattern> regexes = getRegexList();
        if (regexes.size() == 0) {
            return false;
        }

        String str = uri.toString();
        boolean listLogicOR = getListLogicalOr();

        for (Pattern p : regexes) {
            boolean matches = false;
            if (getTimeoutPerRegexSeconds() <= 0) {
                matches = p.matcher(str).matches();
            } else {
                InterruptibleCharSequence interruptible = new InterruptibleCharSequence(
                        str);
                FutureTask<Boolean> matchesFuture = new FutureTask<>(
                        new MatcherTask(p, interruptible));
                ForkJoinPool.commonPool().submit(matchesFuture);
                try {
                    matches = matchesFuture.get(getTimeoutPerRegexSeconds(),
                            TimeUnit.SECONDS);
                } catch (TimeoutException e) {
                    matchesFuture.cancel(true);
                    logger.warning("Timed out after "
                            + getTimeoutPerRegexSeconds()
                            + " seconds waiting for '" + p + "' to match.");
                } catch (InterruptedException e) {
                    matchesFuture.cancel(true);
                    logger.warning("InterruptedException while waiting for '"
                            + p + "' to match.");
                } catch (ExecutionException e) {
                    matchesFuture.cancel(true);
                    logger.warning("ExecutionException while waiting for '" + p
                            + "' to match: " + e.getMessage());
                }
            }

            if (logger.isLoggable(Level.FINER)) {
                logger.finer("Tested '" + str + "' match with regex '"
                        + p.pattern() + " and result was " + matches);
            }

            if (matches) {
                if (listLogicOR) {
                    // OR based and we just got a match, done!
                    logger.fine("Matched: " + str);
                    return true;
                }
            } else {
                if (listLogicOR == false) {
                    // AND based and we just found a non-match, done!
                    return false;
                }
            }
        }

        if (listLogicOR) {
            return false;
        } else {
            return true;
        }
    }

}