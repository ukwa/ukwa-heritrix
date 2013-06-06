package uk.bl.wap.modules.deciderules;

import java.util.Iterator;
import java.util.List;
import java.util.Collection;
import java.util.regex.Pattern;

import org.archive.modules.CrawlURI;
import org.archive.modules.deciderules.DecideResult;
import org.archive.modules.deciderules.MatchesListRegexDecideRule;

/**
 * Rule applies configured decision to a CrawlURI should any of its
 * annotations match the supplied regexs.
 * 
 * @author rcoram
 */
public class AnnotationMatchesListRegexDecideRule extends MatchesListRegexDecideRule {
	private static final long serialVersionUID = 5908003685263995848L;
	
	@Override
	protected boolean evaluate( CrawlURI uri ) {
		List<Pattern> regexList = getRegexList();
		if( regexList.size() == 0 )
			return false;
		
		Collection<String> annotations = uri.getAnnotations();
		boolean listLogicOR = getListLogicalOr();

		Iterator<String> iterator = annotations.iterator();
		while( iterator.hasNext() ) {
			for( Pattern pattern : regexList ) {
				String anno = iterator.next();
				if( pattern.matcher( anno ).matches() ){
					if( listLogicOR ){
						return true;
					}
				} else {
					if( listLogicOR == false ){
						return false;
					}
				}
			}
		}
		
		if( listLogicOR ) {
			return false;
		} else {
			return true;
		}
	}

	@Override
	protected DecideResult innerDecide( CrawlURI uri ) {
		if( evaluate( uri ) ) {
			return getDecision();
		}
		return DecideResult.invert( getDecision() );
	}
}