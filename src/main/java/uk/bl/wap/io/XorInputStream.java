package uk.bl.wap.io;

import java.io.IOException;
import java.io.InputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * uk.bl.wap.io.XorInputStream
 * Provides methods for reading an InputStream while XOR'ing each byte with
 * a given key (defaulting to 'v').
 * @author rcoram
 */

public class XorInputStream extends InputStream {
	private final static Logger LOGGER = Logger.getLogger( XorInputStream.class.getName() );
	private byte key = 'v';
	protected volatile InputStream in;

	public XorInputStream( InputStream in, byte key ) {
		this.in = in;
		this.key = key;
	}

	public XorInputStream( InputStream in ) {
		this.in = in;
	}

	@Override
	public int read() throws IOException {
		int b = in.read();
		if ( b != -1 )
			b = (byte ) ( b ^ key ) & 0xFF;
		return b;
	}
	
	@Override
	public int read( byte b[] ) {
		int i = -1;
		try {
			i = read( b, 0, b.length );			
		} catch( Exception e ) {
			LOGGER.log( Level.SEVERE, e.toString() );
		}
		return i;
	}

	@Override
	public int read( byte b[], int offset, int length ) throws IOException {
		int read = in.read( b, offset, length );
		if( read <= 0 )
			return read;
		for( int i = 0; i < read; i++ ) {
			b[ offset + i ] = ( byte ) ( ( b[ offset + i ] ^ key ) & 0xFF );
		}
		return read;
	}
}
