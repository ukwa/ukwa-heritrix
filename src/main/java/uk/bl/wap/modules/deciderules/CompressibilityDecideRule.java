package uk.bl.wap.modules.deciderules;

import java.util.zip.Deflater;

import org.archive.modules.CrawlURI;
import org.archive.modules.deciderules.PredicatedDecideRule;

public class CompressibilityDecideRule extends PredicatedDecideRule {
    private static final long serialVersionUID = 5661525469638017661L;

    {
	setMin(0.4D);
    }

    public void setMin(double min) {
	kp.put("min", min);
    }

    public double getMin() {
	return (Double) kp.get("min");
    }

    {
	setMax(0.6D);
    }

    public void setMax(double max) {
	kp.put("max", max);
    }

    public double getMax() {
	return (Double) kp.get("max");
    }

    @Override
    protected boolean evaluate(CrawlURI curi) {
	try {
	    Deflater compresser = new Deflater(Deflater.BEST_SPEED);
	    byte[] input = curi.getURI().getBytes("UTF-8");
	    byte[] output = new byte[input.length + 100];
	    compresser.setInput(input);
	    compresser.finish();
	    int compressedDataLength = compresser.deflate(output);
	    compresser.reset();
	    double compressability = ((double) compressedDataLength)
		    / ((double) input.length);
	    return ((compressability > this.getMin()) && (compressability < this
		    .getMax()));
	} catch (Exception e) {
	    e.printStackTrace();
	}
	return false;
    }
}
