package uk.bl.wap.crawler.processor;

import org.archive.crawler.framework.Frontier;
import org.archive.crawler.processor.CrawlMapper;
import org.archive.modules.CrawlURI;
import org.springframework.beans.factory.annotation.Autowired;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

/**
 * Intended as a slightly dumber version of the
 * org.archive.crawler.processor.HashCrawlMapper.
 * 
 * <pre>
 * {@code
 *   <bean id="hashingCrawlMapper" class="uk.bl.wap.crawler.processor.HashingCrawlMapper">
 *     <property name="localName" value="0" />
 *     <property name="crawlerCount" value="4" />
 *     <property name="minimumBits" value="32" />
 *   </bean>
 * </pre>
 * 
 * }
 * 
 * @author rcoram
 * 
 */

public class HashingCrawlMapper extends CrawlMapper {
    private HashFunction hf;

    protected int crawlerCount = 1;
    protected int minimumBits = 32;

    public int getCrawlerCount() {
	return this.crawlerCount;
    }

    public void setCrawlerCount(int count) {
	this.crawlerCount = count;
    }

    public int getMinimumBits() {
	return this.minimumBits;
    }

    public void setMinimumBits(int minimumBits) {
	this.minimumBits = minimumBits;
	this.hf = Hashing.goodFastHash(this.minimumBits);
    }

    protected Frontier frontier;

    public Frontier getFrontier() {
	return this.frontier;
    }

    @Autowired
    public void setFrontier(Frontier frontier) {
	this.frontier = frontier;
    }

    @Override
    protected String map(CrawlURI curi) {
	String key = frontier.getClassKey(curi);
        HashCode hc = hf.hashBytes(key.getBytes());
	int bucket = hc.asInt() % this.getCrawlerCount();
	return Integer.toString(bucket >= 0 ? bucket : -bucket);
    }

}
