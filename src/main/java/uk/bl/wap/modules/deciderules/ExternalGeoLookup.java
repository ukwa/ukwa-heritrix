package uk.bl.wap.modules.deciderules;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.logging.Logger;

import org.archive.modules.deciderules.ExternalGeoLookupInterface;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.City;

/**
 * 
 * @author rcoram
 * <bean id="externalGeoLookup" class="uk.bl.wap.modules.deciderules.ExternalGeoLookup">
 * 	<property name="database" value="/usr/local/share/geoip/geoip-city.mmdb" />
 * </bean>

 * <bean id="externalGeoLookupRule" class="org.archive.modules.deciderules.ExternalGeoLocationDecideRule">
 * 	<property name="lookup">
 * 		<ref bean="externalGeoLookup"/>
 * 	</property>
 * 	<property name="countryCodes"> 
 * 		<list>
 * 			<value>GB</value>
 * 		</list>
 * 	</property>
 * </bean>
 *
 */

public class ExternalGeoLookup implements ExternalGeoLookupInterface {
	private static final long serialVersionUID = -8368728445385838575L;
	private static final Logger LOGGER = Logger.getLogger( ExternalGeoLookup.class.getName() );

	protected String database;

	public String getDatabase() {
		return this.database;
	}

	public void setDatabase( String path ) {
		LOGGER.info( "Database: " + path );
		this.database = path;
	}

	@Override
	public String lookup( InetAddress ip ) {
		try {
			DatabaseReader reader = new DatabaseReader( new File( this.database ) );
			City city = reader.city( InetAddress.getByName( ip.toString().split( "/" )[ 1 ] ) );
			LOGGER.info( "CountryCode: " + city.getCountry().getIsoCode() );
			return city.getCountry().getIsoCode();
		} catch( IOException e ) {
			e.printStackTrace();
		} catch( GeoIp2Exception e ) {
			e.printStackTrace();
		}
		return null;
	}
}