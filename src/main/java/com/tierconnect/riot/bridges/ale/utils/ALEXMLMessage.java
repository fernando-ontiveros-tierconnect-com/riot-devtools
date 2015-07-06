package com.tierconnect.riot.bridges.ale.utils;

import com.tierconnect.riot.bridges.datagen.SequenceNumberService;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

public class ALEXMLMessage
{
	public static final String DFS = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX";

	private static final int TIME_ABS = 0;
	private static final int TIME_REL = 1;
	private static final int TIME_CUM = 2;

	String body;
	String member_loc;
	String member_door;

	String body2;
	StringBuffer members;

	SimpleDateFormat sdf;

	SimpleDateFormat sdf1 = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss z" );

	long epoch = 0;

	int delt = TIME_ABS;

	String[] lastcs = null;

	int state = 0;

	long seqnum;

	boolean realtime = false;

	long beginTime;

	long simTime;

	/**
	 * @param args
	 * @throws IOException
	 * @throws ParseException
	 */
	public static void main( String[] args ) throws IOException, ParseException
	{
		ALEXMLMessage amp = new ALEXMLMessage();
		amp.run( System.in, System.out );
	}

	public ALEXMLMessage() throws IOException
	{
		// TODO: add option to load files from command line
		body = read( "/ale-mojix.xml" );
		member_loc = read( "/ale-mojix-pos.xml" );
		member_door = read( "/ale-mojix-door.xml" );
		sdf = new SimpleDateFormat( DFS );
		seqnum = SequenceNumberService.getSequenceNumber();
		// logger.info( "using seqnum=" + seqnum );
	}

	private String read( String fname ) throws IOException
	{
		InputStream is = ALEXMLMessage.class.getResourceAsStream( fname );
		InputStreamReader isr = new InputStreamReader( is );
		BufferedReader br = new BufferedReader( isr );
		StringBuffer sb = new StringBuffer();
		String line;
		while( (line = br.readLine()) != null )
		{
			// System.out.println( "XML: " + line );
			sb.append( line + "\n" );
		}
		br.close();
		return sb.toString();
	}

	public void run( InputStream in, OutputStream out ) throws IOException, ParseException
	{
		PrintStream ps = new PrintStream( out );

		InputStreamReader isr = new InputStreamReader( in );
		BufferedReader br = new BufferedReader( isr );
		String line;

		beginTime = System.currentTimeMillis();

		while( (line = br.readLine()) != null )
		{
			// System.err.println( "ALEXMLMessage: LINE='" + line + "'" );

			line = line.trim();
			if( !line.isEmpty() )
			{
				String str = convert( line );
				if( str != null && str.length() > 0 )
				{
					pause();

					ps.println( "<!--BEGIN-->\n" );
					System.err.println( "sqn=" + seqnum );
					ps.println( str.trim() );
					ps.println( "<!--END-->\n" );
				}
			}
		}

		body2 = body2.replaceFirst( "\\%MEMBERS\\%", members.toString() );

		pause();

		ps.println( "<!--BEGIN-->" );
		ps.println( body2.trim() );
		ps.println( "<!--END-->" );
		ps.close();

		SequenceNumberService.setSequenceNumber( seqnum );

		br.close();
	}

	private void pause()
	{
		if( realtime )
		{
			//System.err.println( String.format( "%d %d %d %d", simTime, epoch, System.currentTimeMillis(), beginTime ) );
			System.err.println( (simTime - epoch) + " > " + (System.currentTimeMillis() - beginTime) );
			while( (simTime - epoch) > (System.currentTimeMillis() - beginTime) )
			{
				//System.err.println( String.format( "%d %d %d %d", simTime, epoch, System.currentTimeMillis(), beginTime ) );
				System.err.println( (simTime - epoch) + " > " + (System.currentTimeMillis() - beginTime) );
				try
				{
					Thread.sleep( 1000 );
				}
				catch( InterruptedException e )
				{

				}
			}
		}
	}

	public String convert( String line ) throws IOException, ParseException
	{
		StringBuffer sb = new StringBuffer();

		String[] s0 = line.split( "," );
		String[] s = new String[s0.length];

		for( int i = 0; i < s0.length; i++ )
		{
			s[i] = s0[i].trim();
		}

		if( line.startsWith( "#" ) )
		{
			// do nothing
		}
		else if( line.startsWith( "EPOCH" ) )
		{
			// TODO: support TODAY, TODAY+00:00:00, YESTERDAY,
			// YESTERDAY+00:00:00
			if( "NOW".equals( s[1] ) )
			{
				epoch = System.currentTimeMillis();
			}
			else if( "TODAY".equals( s[1] ) )
			{
				Calendar date = new GregorianCalendar();
				// reset hour, minutes, seconds and millis
				date.set( Calendar.HOUR_OF_DAY, 0 );
				date.set( Calendar.MINUTE, 0 );
				date.set( Calendar.SECOND, 0 );
				date.set( Calendar.MILLISECOND, 0 );

				epoch = date.getTimeInMillis();
			}
			else
			{
				Date date = sdf1.parse( s[1] );
				epoch = date.getTime();
			}
			System.err.println( "epoch='" + new Date( epoch ) + "'" );
		}
		else if( line.startsWith( "DELT" ) )
		{
			if( "ABS".equals( s[1] ) )
			{
				delt = TIME_ABS;
			}
			else if( "REL".equals( s[1] ) )
			{
				delt = TIME_REL;
			}
			else if( "CUM".equals( s[1] ) )
			{
				delt = TIME_CUM;
			}
			else
			{
				System.err.println( "invalid delt value '" + s[1] + "'" );
				System.exit( 1 );
			}
			System.err.println( "delt_mode='" + delt + "'" );
		}
		else if( line.startsWith( "REALTIME" ) )
		{
			if( "TRUE".equals( s[1] ) )
			{
				realtime = true;
			}
			else if( "FALSE".equals( s[1] ) )
			{
				realtime = false;
			}
			else
			{
				throw new Error( "unknown REALTIME value='" + s[1] + "'" );
			}
			System.err.println( "realtime='" + realtime + "'" );
		}
		else if( line.startsWith( "CS" ) )
		{
			if( state == 1 )
			{
				body2 = body2.replaceFirst( "\\%MEMBERS\\%", members.toString() );
				sb.append( body2 + "\n" );
				state = 0;
				SequenceNumberService.setSequenceNumber( seqnum );
			}

			if( state == 0 )
			{
				seqnum++;
				body2 = body;
				if( s.length > 1 )
				{
					lastcs = s;
				}
				body2 = body2.replaceFirst( "\\%LONG\\%", lastcs[1] );
				body2 = body2.replaceFirst( "\\%LAT\\%", lastcs[2] );
				body2 = body2.replaceFirst( "\\%ELE\\%", lastcs[3] );
				body2 = body2.replaceFirst( "\\%DEC\\%", lastcs[4] );
				body2 = body2.replaceFirst( "\\%UNITS\\%", lastcs[5] );
				body2 = body2.replaceFirst( "\\%SEQNUM\\%", "" + seqnum );
				members = new StringBuffer();
				state = 1;
			}
		}
		else if( line.startsWith( "LOC" ) )
		{
			// TODO: support wildcard variables (remember previous
			// value)
			String str = member_loc;
			simTime = parseTime( s[1] );
			str = str.replaceFirst( "%TIMESTAMP%", sdf.format( simTime ) );
			str = str.replaceFirst( "%TAG%", s[2] );
			str = str.replaceFirst( "%X%", s[3] );
			str = str.replaceFirst( "%Y%", s[4] );
			str = str.replaceFirst( "%Z%", s[5] );
			str = str.replaceFirst( "%LOGICALREADER%", s[6] );
			str = str.replaceFirst( "%ENODE%", s[7] );
			members.append( str );
		}
		else if( line.startsWith( "DOOR" ) )
		{
			String str = member_door;
			simTime = parseTime( s[1] );
			str = str.replaceFirst( "%TIMESTAMP%", sdf.format( simTime ) );
			str = str.replaceFirst( "%TAG%", s[2] );
			str = str.replaceFirst( "%LOGICALREADER%", s[3] );
			str = str.replaceFirst( "%DIR%", s[4] );
			members.append( str );
		}

		return sb.toString();
	}

	private long parseTime( String str )
	{
		long l = 0;
		if( delt == TIME_ABS )
		{
			l = parseRawTime( str );
		}
		else if( delt == TIME_REL )
		{
			l = epoch + parseRawTime( str );
		}
		else if( delt == TIME_CUM )
		{
			long del = parseRawTime( str );
			epoch += del;
			l = epoch;
		}
		return l;
	}

	private long parseRawTime( String str )
	{
		if( str.contains( ":" ) )
		{
			String[] r = str.split( ":" );
			long hours = Long.parseLong( r[0] );
			long mins = Long.parseLong( r[1] );
			long secs = Long.parseLong( r[2] );
			return 3600L * hours * 1000L + 60L * mins * 1000L + secs * 1000L;
		}
		else
		{
			return Long.parseLong( str );
		}
	}
}
