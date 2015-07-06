package com.tierconnect.riot.bridges.ale.utils;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;

public class ALEPost
{
	String host;
	int port;
	String contextPath;

	int count = 1;

	/**
	 * @param args
	 * @throws IOException
	 * @throws URISyntaxException
	 * @throws ParseException
	 */
	public static void main( String[] args ) throws IOException, URISyntaxException, ParseException
	{
		ALEPost alep = new ALEPost( args );
		alep.run( System.in );
	}

	public ALEPost( String[] args ) throws ParseException
	{
		System.out.println( "intializing ALEPost" );

		Options options = new Options();
		options.addOption( "h", true, "http host (defaults to localhost)" );
		options.addOption( "p", true, "http port (defaults to 9090)" );
		options.addOption( "help", false, "show this help" );
		CommandLineParser parser = new BasicParser();
		CommandLine line = parser.parse( options, args );
		host = line.hasOption( "h" ) ? line.getOptionValue( "h" ) : "localhost";
		port = line.hasOption( "p" ) ? Integer.parseInt( line.getOptionValue( "p" ) ) : 9090;
	}

	public void run( InputStream in ) throws IOException, URISyntaxException
	{
		System.out.println( "running ALEPost" );

		InputStreamReader isr = new InputStreamReader( in );
		BufferedReader br = new BufferedReader( isr );
		String line;
		int state = 0;
		StringBuffer sb = new StringBuffer();
		while( (line = br.readLine()) != null )
		{
			//System.out.println( "ALEPost: line='" + line + "'" );

			if( line.startsWith( "<!--BEGIN" ) )
			{
				state = 1;
			}
			else if( line.startsWith( "<!--END" ) )
			{
				// send message
				// System.out.println( "**** BEGIN" );
				//System.out.println( "body='" + sb.toString().trim() + "'" );
				// System.out.println( "**** END" );
				postMessage( sb.toString().trim() );
				state = 0;
				sb = new StringBuffer();
			}
			else if( state == 1 )
			{
				sb.append( line + "\n" );
			}
		}
		br.close();

		System.out.println( "ALEPost done" );
	}

	public void postMessage( String body ) throws ClientProtocolException, IOException, URISyntaxException
	{
		CloseableHttpClient httpclient = HttpClients.createDefault();

		URIBuilder ub = new URIBuilder();
		ub.setScheme( "http" );
		ub.setHost( host );
		ub.setPort( port );
		// ub.setPath( contextPath + "/api/thing/" );
		ub.setPath( "/" );
		URI uri = ub.build();

		HttpPost httppost = new HttpPost( uri );

		System.out.println( count + " begin executing http POST uri=" + uri );

		StringEntity entity = new StringEntity( body, ContentType.create( "text/plain", "UTF-8" ) );
		httppost.setEntity( entity );
		CloseableHttpResponse response = httpclient.execute( httppost );
		try
		{
			InputStream is = response.getEntity().getContent();
			InputStreamReader isr = new InputStreamReader( is );
			BufferedReader br = new BufferedReader( isr );
			String line;
			while( (line = br.readLine()) != null )
			{
				System.out.println( "line=" + line );
			}
		}
		finally
		{
			response.close();
		}

		System.out.println( count + " done executing http POST uri=" + uri );
		count++;
	}
}
