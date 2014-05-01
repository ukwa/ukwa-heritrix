package uk.bl.wap.util;

/**
 * Comments taken from clamd man page.
 * @author rcoram
 * 
 */

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ClamdScanner {
	public static final int DEFAULT_CHUNK_SIZE = 64 * 1024; 
	public static final byte[] INSTREAM = "zINSTREAM\0".getBytes();
	public static final byte[] VERSION = "zVERSION\0".getBytes();
	public static final byte[] SESSION = "zIDSESSION\0".getBytes();
	public static final byte[] END = "zEND\0".getBytes();
	private final static Logger LOGGER = Logger.getLogger( ClamdScanner.class.getName() );

	private InetSocketAddress inetSocket;
	private int timeout;
	private int streamMaxLength = -1;

	public ClamdScanner() {
		this.inetSocket = new InetSocketAddress( "localhost", 3310 );
		this.timeout = 0;
	}

	public ClamdScanner( String host, int port, int timeout ) {
		this.inetSocket = new InetSocketAddress( host, port );
		this.timeout = timeout;
	}

	public ClamdScanner( String host, int port, int timeout, int streamMaxLength ) {
		this.inetSocket = new InetSocketAddress( host, port );
		this.timeout = timeout;
		this.streamMaxLength = streamMaxLength;
	}	

	protected void setTimeout( int timeout ) {
		this.timeout = timeout;
	}

	public String clamdScan( InputStream input ) {
		return this.clamdSession( input, false );
	}

	/**
	 * @return <CODE>String</CODE> representation of output from clamd
	 */
	public String clamdSession( InputStream input, boolean getVersion ) {
		Socket socket = new Socket();
		DataOutputStream output = null;
		StringBuffer result = new StringBuffer();
		try {
			socket.connect( inetSocket, this.timeout );
			socket.setSoTimeout( this.timeout );

			output = new DataOutputStream( socket.getOutputStream() );
			byte[] buffer = new byte[ DEFAULT_CHUNK_SIZE ];
			int read = -1;

			output.write( SESSION );

			// It's recommended to prefix clamd commands with the letter z...to
			// indicate that the command
			// will be delimited by a NULL character...
			output.write( INSTREAM );
			int total = 0;
			try {
				while( ( read = input.read( buffer ) ) != -1 ) {
				    	total += read;
					try {
						// The format of the chunk is: '<length><data>'...
						output.writeInt( read );
						output.write( buffer, 0, read );
						output.flush();
						if( streamMaxLength != -1 && total >= streamMaxLength ) {
							break;
						}
					} catch( IOException e ) {
						LOGGER.log( Level.WARNING, "Error writing to DataOutputStream: " + e.toString() );
						break;
					}
				}
			} catch( IOException e ) {
				LOGGER.log( Level.SEVERE, "Error reading from InputStream: " + e.toString(), e );
			}
			// Streaming is terminated by sending a zero-length chunk.
			output.writeInt( 0 );
			if( getVersion )
				output.write( VERSION );
			output.write( END );
			output.flush();

			result.append( getResponse( socket ) );
			if( getVersion ) {
				result.append( "," );
				result.append( getResponse( socket ) );
			}
		} catch( Exception e ) {
			LOGGER.log( Level.WARNING, "Error connecting to clamd: " + e );
		} finally {
			if( output != null ) {
				try {
					output.close();
				} catch( IOException e ) {
					LOGGER.log( Level.WARNING, "Error closing DataOutputStream: " + e.toString() );
				}
			}
			try {
				socket.close();
			} catch( IOException e ) {
				LOGGER.log( Level.WARNING, "Error closing socket: " + e.toString() );
			}
		}
		return result.toString();
	}

	private String getResponse( Socket socket ) {
		StringBuffer response = new StringBuffer();
		byte[] buffer = new byte [ 1 ];
		while( response.length() <= 128 ) {
			try {
				socket.getInputStream().read( buffer );
			} catch( IOException e ) {
				LOGGER.log( Level.WARNING, "ClamdScanner.getResponse(): " + e.toString() );
				break;
			}
			// Clamd replies will honour the requested terminator...
			if( buffer[ 0 ] == '\0' )
				break;
			response.append( new String( buffer ) );
		}
		return response.toString();
	}
}