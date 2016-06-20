/**
 * 
 */
package uk.bl.wap.modules.extractor;

import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.zip.DataFormatException;

import org.springframework.context.annotation.Import;

import com.flagstone.transform.DoAction;
import com.flagstone.transform.EventHandler;
import com.flagstone.transform.Movie;
import com.flagstone.transform.MovieTag;
import com.flagstone.transform.Place2;
import com.flagstone.transform.action.Action;
import com.flagstone.transform.action.GetUrl;
import com.flagstone.transform.action.GetUrl2;
import com.flagstone.transform.movieclip.DefineMovieClip;
import com.flagstone.transform.movieclip.InitializeMovieClip;

/**
 * 
 * Exploring a possible replacement for:
 * @see org.archive.modules.extractor.ExtractorSWF.java
 * 
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class ExtractorTransformSWF {

	public static void main( String[] args ) throws DataFormatException, IOException {
		Movie movie = new Movie();		
		movie.decodeFromStream( new URL("http://wayback.archive-it.org/779/20080709003013/http://www.dreamingmethods.com/uploads/lastdream/loader.swf").openStream() );
		goThroughTags(movie.getObjects());
	}

	private static void goThroughTags( List<MovieTag> tags ) {
		for( MovieTag tag : tags ) {
			if( tag instanceof Import ) {
				Import im = (Import) tag;
				System.out.println("URL:" + im.value()); // was im.getUrl()
			} else if( tag instanceof DefineMovieClip ) {
				DefineMovieClip dmc = (DefineMovieClip) tag;
				System.out.println("DMC:" + dmc.getIdentifier() );
				goThroughTags(dmc.getObjects());
			} else if( tag instanceof DoAction ) {
				DoAction da = (DoAction) tag;
				goThroughActions(da.getActions());
			} else if( tag instanceof Place2 ) {
				Place2 p2 = (Place2) tag;
				goThroughEvents(p2.getEvents());
			} else if (tag instanceof InitializeMovieClip) {
				InitializeMovieClip imc = (InitializeMovieClip) tag;
			} else {
				System.out.println("Did not parse Tag "+tag.getClass().getSimpleName());
			}
		}
	}

	private static void goThroughEvents(List<EventHandler> events) {
		for( EventHandler eh : events ) {
			System.out.println("Parsing event actions...");
			goThroughActions(eh.getActions());
			//for( Event e : eh.getEvents() ) {
			//	e.get
			//}
			System.out.println("Done.");
		}
	}

	private static void goThroughActions(List<Action> actions) {
		for( Action a : actions ) {
			if( a instanceof GetUrl ) {
				GetUrl gu = (GetUrl) a;
				System.out.println("URL:" + gu.getUrl() );
			} else if( a instanceof GetUrl2 ) {
				GetUrl2 gu = (GetUrl2) a;
				System.out.println("URL:" + gu.getRequest() );
			} else {
				System.out.println("Did not parse Action "+a.getClass().getSimpleName());
			}
		}
	}
}
