package com.tierconnect.riot.bridges.datagen;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class SequenceNumberService
{
	static String fname = "sequenceNumber.txt";

	public static long getSequenceNumber() throws IOException
	{
		File f = new File( fname );

		if( !f.exists() )
		{
			setSequenceNumber( 0 );
			return 0;
		}

		FileReader fr = new FileReader( f );
		BufferedReader br = new BufferedReader( fr );
		StringBuffer sb = new StringBuffer();
		String line;
		while( (line = br.readLine()) != null )
		{
			sb.append( line );
		}
		br.close();

		String s = sb.toString().trim();
		return Long.parseLong( s );
	}

	public static void setSequenceNumber( long seqnum ) throws IOException
	{
		File f = new File( fname );
		FileWriter fw = new FileWriter( f );
		BufferedWriter bw = new BufferedWriter( fw );
		bw.write( "" + seqnum + "\n" );
		bw.close();
	}
}
