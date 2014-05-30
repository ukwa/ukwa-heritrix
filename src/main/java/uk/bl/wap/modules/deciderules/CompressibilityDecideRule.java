package uk.bl.wap.modules.deciderules;

import java.io.UnsupportedEncodingException;
import java.util.logging.Level;
import java.util.logging.Logger;

import java.util.zip.Deflater;

import org.archive.modules.CrawlURI;
import org.archive.modules.deciderules.DecideResult;
import org.archive.modules.deciderules.PredicatedDecideRule;

/**
 * DecideRule which rejects URIs which fall below or above a min/max value
 * respectively, said value being the ratio of the original URI's length versus
 * its compressed length.
 * 
 * @author rcoram
 * 
 */

public class CompressibilityDecideRule extends PredicatedDecideRule {
    private static final long serialVersionUID = 5661525469638017661L;
    private static final Logger LOGGER = Logger
	    .getLogger(CompressibilityDecideRule.class.getName());
    private final Deflater compresser = new Deflater(Deflater.BEST_SPEED);
    {
	setDecision(DecideResult.REJECT);
    }

    {
	setMin(0.28D);
    }

    public void setMin(double min) {
	kp.put("min", min);
    }

    public double getMin() {
	return (Double) kp.get("min");
    }

    {
	setMax(1.6D);
    }

    public void setMax(double max) {
	kp.put("max", max);
    }

    public double getMax() {
	return (Double) kp.get("max");
    }

    @Override
    public DecideResult onlyDecision(CrawlURI uri) {
	return this.getDecision();
    }

    @Override
    protected boolean evaluate(CrawlURI curi) {
	try {
	    byte[] input = curi.getURI().getBytes("UTF-8");
	    byte[] output = new byte[input.length + 100];
	    int compressedDataLength;
	    synchronized (compresser) {
		compresser.setInput(input);
		compresser.finish();
		compressedDataLength = compresser.deflate(output);
		compresser.reset();
	    }
	    double compressibility = ((double) compressedDataLength)
		    / ((double) input.length);
	    boolean result = ((compressibility < this.getMin()) || (compressibility > this
		    .getMax()));
	    return result;
	} catch (UnsupportedEncodingException e) {
	    LOGGER.log(Level.WARNING, curi.getURI(), e);
	    curi.getNonFatalFailures().add(e);
	}
	return false;
    }
}
