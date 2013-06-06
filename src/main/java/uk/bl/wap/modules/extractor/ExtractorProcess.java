package uk.bl.wap.modules.extractor;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.archive.modules.CrawlURI;
import org.archive.modules.extractor.ContentExtractor;
import org.archive.modules.extractor.HTMLLinkContext;
import org.archive.modules.extractor.Hop;
import org.archive.modules.extractor.Link;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @author rcoram
 * 
 */

public class ExtractorProcess extends ContentExtractor {
	private final static int MAX_OUTLINKS = 4096;
	private final static String ANNOTATION = "ProcessExtractor";
	private static Logger logger = Logger.getLogger( ExtractorProcess.class.getName() );
	private final static String SLASH_PAGE = "^https?://?[^/]+/$";
	private Pattern pattern;

	{
		setArgs( new ArrayList<String>() );
	}
	@SuppressWarnings( "unchecked" )
	public ArrayList<String> getArgs() {
		return ( ArrayList<String> ) kp.get( "args" );
	}
	@Autowired
	public void setArgs( ArrayList<String> args ) {
		kp.put( "args", args );
	}

	public ExtractorProcess() {
		this.pattern = Pattern.compile( SLASH_PAGE );
	}

	@Override
	protected boolean innerExtract( CrawlURI curi ) {
		BufferedInputStream stdout = null;
		ByteArrayOutputStream json = new ByteArrayOutputStream();
		try {
			ArrayList<String> args = this.getArgs();
			args.add( curi.getURI() );
			ProcessBuilder builder = new ProcessBuilder( args );
			Process process = builder.start();
			byte[] buffer = new byte[ 4096 ];
			stdout = new BufferedInputStream( process.getInputStream() );
			
			//PhantomJs bug #150
			BufferedReader reader = new BufferedReader( new InputStreamReader( stdout ) );
			while( !reader.readLine().equals( "{" ) ) {
				//Console errors are written to stdout in WebKit
			}
			json.write( "{".getBytes() );

			int i;
			while( ( i = stdout.read( buffer ) ) > 0 ) {
				json.write( buffer, 0, i );
			}
			Iterator<String> newUris = this.parseJson( json.toString() ).iterator();
			while( newUris.hasNext() ) {
				Link.addRelativeToBase( curi, MAX_OUTLINKS, newUris.next(), HTMLLinkContext.EMBED_MISC, Hop.EMBED );
			}
			curi.getAnnotations().add( ANNOTATION );
		} catch( Exception e ) {
			logger.warning( e.getMessage() );
		} finally {
			try {
				stdout.close();
			} catch( IOException i ) {
				logger.warning( i.getMessage() );
			}
			try {
				json.close();
			} catch( IOException i ) {
				logger.warning( i.getMessage() );
			}
		}
		return false;
	}

	@Override
	protected boolean shouldExtract( CrawlURI curi ) {
		if( curi.isSeed() )
			return true;
		Matcher matcher = this.pattern.matcher( curi.getURI() );
		return matcher.matches();
	}

	private ArrayList<String> parseJson( String jsonString ) throws JSONException {
		ArrayList<String> outlinks = new ArrayList<String>();
		JSONObject json = new JSONObject( jsonString );
		JSONObject log = json.getJSONObject( "log" );
		JSONArray entries = log.getJSONArray( "entries" );
		int i = 0;
		while( i < entries.length() ) {
			JSONObject request = ( JSONObject) entries.getJSONObject( i ).get( "request" );
			outlinks.add( ( String ) request.get( "url" ) );
			i++;
		}
		return outlinks;
	}

	public static void main( String[] args ) throws Exception {
		ProcessBuilder builder = new ProcessBuilder( "C:/phantomjs/phantomjs.exe", "C:/phantomjs/examples/netsniff.js", "http://www.google.co.uk/" );
		Process process = builder.start();
		byte[] buffer = new byte[ 4096 ];
		BufferedInputStream stdout = new BufferedInputStream( process.getInputStream() );
		ByteArrayOutputStream output = new ByteArrayOutputStream();

		BufferedReader reader = new BufferedReader( new InputStreamReader( stdout ) );
		String line = reader.readLine();
		while( !line.startsWith( "{" ) ) {
			System.err.println( "DURH: " + line );
		}
		output.write( "{".getBytes() );

		int i;
		while( ( i = stdout.read( buffer ) ) > 0 ) {
			output.write( buffer, 0, i );
		}
		System.out.println( output.toString() );
		JSONObject json = new JSONObject( output.toString() );
		JSONObject log = json.getJSONObject( "log" );
		JSONArray entries = log.getJSONArray( "entries" );
		i = 0;
		while( i < entries.length() ) {
			JSONObject request = ( JSONObject) entries.getJSONObject( i ).get( "request" );
			System.out.println( request.get( "url" ) );
			i++;
		}
	}
}
