/**
 * 
 */
package uk.bl.wap.crawler.reporting;

import java.io.IOException;
import java.io.PrintWriter;

import org.archive.crawler.reporting.Report;
import org.archive.crawler.reporting.StatisticsTracker;
import org.springframework.beans.factory.annotation.Autowired;

import uk.bl.wap.crawler.frontier.KafkaUrlReceiver;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class KafkaUrlReceiverReport extends Report {

    protected KafkaUrlReceiver kafkaUrlReceiver;

    public KafkaUrlReceiver getKafkaUrlReceiver() {
        return kafkaUrlReceiver;
    }

    @Autowired
    public void setKafkaUrlReceiver(KafkaUrlReceiver kafkaUrlReceiver) {
        this.kafkaUrlReceiver = kafkaUrlReceiver;
    }

    /* (non-Javadoc)
     * @see org.archive.crawler.reporting.Report#write(java.io.PrintWriter, org.archive.crawler.reporting.StatisticsTracker)
     */
    @Override
    public void write(PrintWriter writer, StatisticsTracker stats) {
        try {
            this.kafkaUrlReceiver.reportTo(writer);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    /* (non-Javadoc)
     * @see org.archive.crawler.reporting.Report#getFilename()
     */
    @Override
    public String getFilename() {
        return "kafka-url-receiver-report.txt";
    }

}
