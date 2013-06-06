package uk.bl.wap.net;

import java.io.ByteArrayOutputStream;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.soap.MessageFactory;
import javax.xml.soap.SOAPBody;
import javax.xml.soap.SOAPConnection;
import javax.xml.soap.SOAPConnectionFactory;
import javax.xml.soap.SOAPConstants;
import javax.xml.soap.SOAPElement;
import javax.xml.soap.SOAPEnvelope;
import javax.xml.soap.SOAPException;
import javax.xml.soap.SOAPHeader;
import javax.xml.soap.SOAPMessage;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;

import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

/**
 * Used for creating SOAP message specifically tailored to the requirements
 * of the Link Repository.
 * 
 * @author rcoram
 *
 */

public class SOAPUtil {
	private static final Logger LOGGER = Logger.getLogger( SOAPUtil.class.getName() );
	
	public static SOAPMessage createMessage() {
		SOAPMessage message = null;
		try {
			MessageFactory messageFactory = MessageFactory.newInstance( SOAPConstants.SOAP_1_2_PROTOCOL );
			message = messageFactory.createMessage();
			SOAPEnvelope envelope = message.getSOAPPart().getEnvelope();
			envelope.setPrefix( "soap12" );
			envelope.removeNamespaceDeclaration( "env" );
			envelope.addNamespaceDeclaration( "xsi", "http://www.w3.org/2001/XMLSchema-instance" );
			envelope.addNamespaceDeclaration( "xsd", "http://www.w3.org/2001/XMLSchema" );
			envelope.addNamespaceDeclaration( "soap12", "http://www.w3.org/2003/05/soap-envelope" );

			SOAPHeader header = envelope.getHeader();
			header.setPrefix( "soap12" );

			SOAPBody body = envelope.getBody();
			body.setPrefix( "soap12" );
			message.saveChanges();
		} catch( SOAPException e ) {
			LOGGER.log( Level.SEVERE, "createMessage: " + e.toString() );
		}
		return message;
	}
	
	public static SOAPMessage sendMessage( SOAPMessage message, String URL ) {
		SOAPMessage reply = null;
		try {
			SOAPConnectionFactory soapConnFactory = SOAPConnectionFactory.newInstance();
			SOAPConnection connection = soapConnFactory.createConnection();
			reply = connection.call( message, URL );
			connection.close();
		} catch( SOAPException e ) {
			LOGGER.log( Level.SEVERE, "sendMessage: " + e.toString() );
		}
		return reply;
	}
	
	public static NodeList getNodes( ByteArrayOutputStream output, String node ) throws Exception {
		DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
		XPath xpath = XPathFactory.newInstance().newXPath();
		Document document = builder.parse( new InputSource( new StringReader( output.toString() ) ) );
	    XPathExpression expression = xpath.compile( node );
	    Object result = expression.evaluate( document, XPathConstants.NODESET );
	    return ( NodeList ) result;
	}
	
	public static ArrayList<String> getUncrawledSeeds( String imaxResults, String crawlerId, String crawlerPoolsize, String URL ) {
		ArrayList<String> seeds = new ArrayList<String>();
		try {
			SOAPMessage message = createMessage();
			SOAPBody body = message.getSOAPBody();
			SOAPElement bodyElement =  body.addChildElement( "getUncrawledSeeds", "", "http://www.bl.uk/webarchive/" );
			bodyElement.addChildElement( "imaxResults" ).addTextNode( imaxResults );
			bodyElement.addChildElement( "crawler_id" ).addTextNode( crawlerId );
			bodyElement.addChildElement( "crawler_poolsize" ).addTextNode( crawlerPoolsize );
			message.saveChanges();
			
			SOAPMessage reply = sendMessage( message, URL );
			ByteArrayOutputStream output = new ByteArrayOutputStream();
			reply.writeTo( output );

			NodeList nodes = getNodes( output, "//@http_uri");
		    for( int i = 0; i < nodes.getLength(); i++ ) {
		    	seeds.add( nodes.item( i ).getNodeValue() );
		    }
		} catch( Exception e ) {
			LOGGER.log( Level.SEVERE, "getUncrawledSeeds: " + e.toString() );
		}
		return seeds;
	}
	
	public static int LogBatchPIPE( String records, String crawlerId, String URL ) {
		int numRecords = -1;
		try {
			SOAPMessage message = createMessage();
			SOAPBody body = message.getSOAPBody();
			SOAPElement bodyElement =  body.addChildElement( "LogBatchPIPE", "", "http://www.bl.uk/webarchive/" );
			bodyElement.addChildElement( "strLogRecordPIPE" ).addTextNode( records );
			bodyElement.addChildElement( "crawler_id" ).addTextNode( crawlerId );
			message.saveChanges();
			
			SOAPMessage reply = sendMessage( message, URL );
			numRecords = Integer.parseInt( reply.getSOAPBody().getTextContent() );
		} catch( Exception e ) {
			LOGGER.log( Level.SEVERE, "LogBatchPIPE: " + e.toString() );
		}
		return numRecords;
	}
	
	public static ArrayList<String> getSiteLinks( String seed, String crawlerId, String URL ) {
		ArrayList<String> links = new ArrayList<String>();
		try {
			SOAPMessage message = createMessage();
			SOAPBody body = message.getSOAPBody();
			SOAPElement bodyElement =  body.addChildElement( "getSiteLinks", "", "http://www.bl.uk/webarchive/" );
			bodyElement.addChildElement( "http_uri" ).addTextNode( seed );
			bodyElement.addChildElement( "crawler_id" ).addTextNode( crawlerId );
			message.saveChanges();

			SOAPMessage reply = sendMessage( message, URL );
			ByteArrayOutputStream output = new ByteArrayOutputStream();
			reply.writeTo( output );

			NodeList nodes = getNodes( output, "//@http_uri" );
		    for( int i = 0; i < nodes.getLength(); i++ ) {
		    	links.add( nodes.item( i ).getNodeValue() );
		    }
		}  catch( Exception e ) {
			LOGGER.log( Level.SEVERE, "getSiteLinks: " + e.toString() );
		}
		return links;
	}
}
