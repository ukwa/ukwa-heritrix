package uk.bl.wap.modules.extractor;

import static org.archive.modules.extractor.Hop.SPECULATIVE;
import static org.archive.modules.extractor.LinkContext.SPECULATIVE_MISC;

import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.archive.modules.CrawlURI;
import org.archive.modules.extractor.ContentExtractor;
import org.archive.modules.extractor.Link;
import org.archive.util.UriUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;

/**
 * Extracts URIs from JSON resources.
 * 
 * @author rcoram
 *
 */

public class ExtractorJson extends ContentExtractor {
	public final static String JSON_URI = "^https?://[^/]+/.+\\.json\\b.*$";

	@Override
	protected boolean innerExtract( CrawlURI curi ) {
		try {
			JSONTokener tokener = new JSONTokener( new InputStreamReader( curi.getRecorder().getContentReplayInputStream() ) );
			JSONObject json = new JSONObject( tokener );
			Map<String, String> map = new HashMap<String, String>();
			parse( json, map );
			String link = null;
			for( String key : map.keySet() ) {
				link = map.get( key );
				if( UriUtils.isLikelyUri( map.get( key ) ) ) {
					Link.addRelativeToBase( curi, this.getExtractorParameters().getMaxOutlinks(), link, SPECULATIVE_MISC, SPECULATIVE );
				}
			}
		} catch( Exception e ) {
			curi.getNonFatalFailures().add( e );
		}
		return false;
	}

	@Override
	protected boolean shouldExtract( CrawlURI curi ) {
		String contentType = curi.getContentType();
		if( contentType != null && contentType.indexOf( "json" ) != -1 ) {
			return true;
		}

		if( curi.toString().matches( JSON_URI ) ) {
			return true;
		}
		return false;
	}

	@SuppressWarnings( "unchecked" )
	private static Map<String, String> parse( JSONObject json, Map<String, String> map ) throws JSONException {
		Iterator<String> keys = json.keys();
		while( keys.hasNext() ) {
			String key = keys.next();
			String val = null;
			try {
				JSONObject value = json.getJSONObject( key );
				parse( value, map );
			} catch( Exception e ) {
				try {
					JSONArray value = json.getJSONArray( key );
					for( int i = 0; i < value.length(); i++ ) {
						parse( value.getJSONObject( i ), map );
					}
				} catch( Exception f ) {
					val = json.getString( key );
				}
			}
			if( val != null ) {
				map.put( key, val );
			}
		}
		return map;
	}
}