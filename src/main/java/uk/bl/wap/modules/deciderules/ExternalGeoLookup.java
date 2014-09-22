package uk.bl.wap.modules.deciderules;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.logging.Logger;

import org.archive.modules.deciderules.ExternalGeoLookupInterface;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CityResponse;

/**
 * <pre>
 * {@code
 *   <bean id="externalGeoLookup" class="uk.bl.wap.modules.deciderules.ExternalGeoLookup">
 *     <property name="database" value="/usr/local/share/geoip/geoip-city.mmdb" />
 *   </bean>
 * </pre>
 * 
 * }
 * 
 * @author rcoram
 */

public class ExternalGeoLookup implements ExternalGeoLookupInterface {
    private static final long serialVersionUID = -8368728445385838575L;
    private static final Logger LOGGER = Logger
	    .getLogger(ExternalGeoLookup.class.getName());

    protected String database;
    protected DatabaseReader reader;

    public String getDatabase() {
	return this.database;
    }

    public void setDatabase(String path) throws IOException {
	database = path;
	LOGGER.info("Database: " + database);
	reader = new DatabaseReader.Builder(new File(database)).build();
    }

    @Override
    public String lookup(InetAddress ip) {
	try {
	    CityResponse city = reader.city(ip);
	    if (city != null) {
		return city.getCountry().getIsoCode();
	    }
	} catch (IOException e) {
	    LOGGER.warning(e.getMessage());
	} catch (GeoIp2Exception e) {
	    LOGGER.warning(e.getMessage());
	}
	return null;
    }
}