/**
 * 
 */
package uk.bl.wap.crawler.postprocessor;

import static org.archive.modules.fetcher.FetchStatusCodes.S_DEFERRED;
import static org.archive.modules.fetcher.FetchStatusCodes.S_PREREQUISITE_UNSCHEDULABLE_FAILURE;

import org.archive.crawler.postprocessor.CandidatesProcessor;
import org.archive.modules.CrawlURI;
import org.archive.modules.extractor.Hop;
import org.archive.spring.KeyedProperties;

import uk.bl.wap.crawler.frontier.KafkaUrlReceiver;

/**
 * 
 * Works like the standard CandidatesProcessor but does not directly place links
 * into the frontier.
 * 
 * Intended to be followed by a process that sends URIs to a queue.
 * 
 * @see CandidatesProcessor
 * @see KafkaKeyedToCrawlFeed
 * 
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class PassthroughCandidatesProcessor extends CandidatesProcessor {

    /**
     * Run candidatesChain on a single candidate CrawlURI; if its reported
     * status is nonnegative, schedule to frontier.
     * 
     * Also applies special handling of discovered URIs that by convention we
     * want to treat as seeds (which then may be scheduled indirectly via
     * addSeed).
     * 
     * @param candidate
     *            CrawlURI to consider
     * @param source
     *            CrawlURI from which candidate was discovered/derived
     * @return candidate's status code at end of candidate chain execution
     * @throws InterruptedException
     */
    public int runCandidateChain(CrawlURI candidate, CrawlURI source)
            throws InterruptedException {
        // at least for duration of candidatechain, offer
        // access to full CrawlURI of via
        candidate.setFullVia(source);
        sheetOverlaysManager.applyOverlaysTo(candidate);
        try {
            KeyedProperties.clearOverridesFrom(source);
            KeyedProperties.loadOverridesFrom(candidate);

            // apply special seed-status promotion
            if (getSeedsRedirectNewSeeds() && source != null && source.isSeed()
                    && candidate.getLastHop().equals(Hop.REFER.getHopString())
                    && candidate
                            .getHopCount() < SEEDS_REDIRECT_NEW_SEEDS_MAX_HOPS) {
                candidate.setSeed(true);
            }

            getCandidateChain().process(candidate, null);
            int statusAfterCandidateChain = candidate.getFetchStatus();
            if (statusAfterCandidateChain >= 0) {
                if (checkForSeedPromotion(candidate)) {
                    /*
                     * We want to guarantee crawling of seed version of CrawlURI
                     * even if same url has already been enqueued, see
                     * https://webarchive.jira.com/browse/HER-1891
                     */
                    candidate.setForceFetch(true);
                    candidate.setSeed(true);
                    // Do nothing as we will schedule in the KafkaUrlReciever:
                    // getSeeds().addSeed(candidate); // triggers scheduling
                } else {
                    // Only enqueue URLs routed here via Kafka,
                    // except for prerequisites which are handled internally:
                    if (candidate.isPrerequisite() || candidate.getAnnotations()
                            .contains(KafkaUrlReceiver.A_RECEIVED_FROM_KAFKA)) {
                        frontier.schedule(candidate);
                    }
                }
            }
            return statusAfterCandidateChain;
        } finally {
            KeyedProperties.clearOverridesFrom(candidate);
            KeyedProperties.loadOverridesFrom(source);
        }
    }

    /**
     * Run candidates chain on each of (1) any prerequisite, if present; (2) any
     * outCandidates, if present; (3) all outlinks, if appropriate
     * 
     * @see org.archive.modules.Processor#innerProcess(org.archive.modules.CrawlURI)
     */
    @Override
    protected void innerProcess(final CrawlURI curi)
            throws InterruptedException {
        // (1) Handle any prerequisites when S_DEFERRED for prereqs
        if (curi.hasPrerequisiteUri() && curi.getFetchStatus() == S_DEFERRED) {
            CrawlURI prereq = curi.getPrerequisiteUri();

            int prereqStatus = runCandidateChain(prereq, curi);

            if (prereqStatus < 0) {
                curi.setFetchStatus(S_PREREQUISITE_UNSCHEDULABLE_FAILURE);
            }
            return;
        }

        // Only consider candidate links of error pages if configured to do so
        if (!getProcessErrorOutlinks() && (curi.getFetchStatus() < 200
                || curi.getFetchStatus() >= 400)) {
            curi.getOutLinks().clear();
            return;
        }

        // (3) Handle outlinks (usual bulk of discoveries)
        for (CrawlURI candidate : curi.getOutLinks()) {

            runCandidateChain(candidate, curi);

        }

    }

}
