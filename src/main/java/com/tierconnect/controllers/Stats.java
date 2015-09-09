package com.tierconnect.controllers;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.tierconnect.dev.controllerInterface;
import com.tierconnect.utils.CommonUtils;
import com.tierconnect.utils.MqttUtils;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Scanner;

/**
 * Created by fernando on 9/3/15.
 */
public class stats implements controllerInterface
{
	CommonUtils cu;
	DBCollection thingsCollection;
	DBCollection statsCollection;
	MqttUtils mq;

	int times = 0;
	long sinit, sloop1, snative, sesper, sfmc, supdate, ssave, sseries, sparent, stotal, sthings, sfields = 0;
	public void setCu(CommonUtils cu) {
		this.cu = cu;
	}


	public String getDescription() {

		return "Statistics";
	}

	public void setup()
	{
		thingsCollection        = cu.db.getCollection("things");
		statsCollection        = cu.db.getCollection("ALEBLog");

	}

	private Boolean isValidOption (String str, ArrayList<String> items) {
		boolean contains = false;
		for (String item : items) {
			if (str.equalsIgnoreCase(item)) {
				contains = true;
				break; // No need to look further.
			}
		}
		return contains;
	}


	private void displayALEBlogHeader()
	{
		System.out.println ("+--------+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-------+----+------+----+------+");
		System.out.println ("|  time  | sqn |init |loop1|nativ|esper| fmc |updat|save |serie|paren|total|created|  t |  t/s |  f |  f/s |");

	}

	private void displayALEBlogFooter()
	{
		System.out.println ("+--------+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-------+----+------+----+------+");

		System.out.print( " " + cu.rtrim( "",8)  );
		System.out.print( " " + cu.rtrim( "",5)  );
		System.out.print( " " + cu.rtrim( sinit,5 ) + " " + cu.rtrim(sloop1, 5)  );
		System.out.print( " " + cu.rtrim( snative,5) + " " + cu.rtrim( sesper,5 ) + " " + cu.rtrim(sfmc, 5)  );
		System.out.print( " " + cu.rtrim( supdate,5) + " " + cu.rtrim( ssave,5 ) + " " + cu.rtrim(sseries, 5)  );
		System.out.print( " " + cu.rtrim( sparent,5) + " " + cu.rtrim( stotal,5 ) + " " + cu.rtrim( "",7) );

		long ts = (long) (1000 * sthings  / stotal);
		long fs = (long) (1000 * sfields  / stotal);

		System.out.print( " " + cu.rtrim(sthings, 4) + " " + cu.rtrim( ts, 6 ));
		System.out.print( " " + cu.rtrim(sfields, 4) + " " + cu.rtrim( fs, 6 ) );
		System.out.println();
	}

	private Date displayALEBlogRecords(Date time)
	{
		BasicDBObject queryDoc = new BasicDBObject( "time", new BasicDBObject( "$gt", time )  );
		BasicDBObject sortDoc = new BasicDBObject( "time", 1  );
		DBCursor cursor = statsCollection.find( queryDoc ).sort( sortDoc );

		while (cursor.hasNext()) {
			DBObject doc = cursor.next();
			System.out.print( "|" + cu.rtrim( doc.get("time").toString().substring(11,19 ),8)  );
			System.out.print( "|" + cu.rtrim( doc.get("sqn"),5)  );
			System.out.print( "|" + cu.rtrim( doc.get("init"),5 ) + "|" + cu.rtrim(doc.get("loop1"), 5)  );
			System.out.print( "|" + cu.rtrim( doc.get("native"),5) + "|" + cu.rtrim( doc.get("esper"),5 ) + "|" + cu.rtrim(doc.get("fmc"), 5)  );
			System.out.print( "|" + cu.rtrim( doc.get("update"),5) + "|" + cu.rtrim( doc.get("save"),5 ) + "|" + cu.rtrim(doc.get("history"), 5)  );
			System.out.print( "|" + cu.rtrim( doc.get("parent"),5) + "|" + cu.rtrim( doc.get("total"),5 ) + "|" + cu.rtrim( doc.get("thingsCreated"),7) );

			long ts = (long) (1000 * Long.valueOf( doc.get("things").toString() )  / Long.valueOf( doc.get("total").toString() ));
			long fs = (long) (1000 * Long.valueOf( doc.get("fields").toString() )  / Long.valueOf( doc.get("total").toString() ));

			System.out.print( "|" + cu.rtrim( doc.get("things"), 4) + "|" + cu.rtrim( ts, 6 ));
			System.out.print( "|" + cu.rtrim( doc.get("fields"), 4) + "|" + cu.rtrim( fs, 6 ) );
			System.out.println("|");

			time = (Date)doc.get( "time" );

			times ++;

			sinit   += Long.valueOf( doc.get("init").toString());
			sloop1  += Long.valueOf( doc.get("loop1").toString());
			snative += Long.valueOf( doc.get("native").toString());
			sesper  += Long.valueOf( doc.get("esper").toString());
			sfmc    += Long.valueOf( doc.get("fmc").toString());
			supdate += Long.valueOf( doc.get("update").toString());
			ssave   += Long.valueOf( doc.get("save").toString());
			sseries += Long.valueOf( doc.get("history").toString());
			sparent += Long.valueOf( doc.get("parent").toString());
			stotal += Long.valueOf( doc.get("total").toString());
			sthings += Long.valueOf( doc.get("things").toString());
			sfields += Long.valueOf( doc.get("fields").toString());
			if (times % 10 == 0 ) {
				displayALEBlogFooter();
				displayALEBlogHeader();
			}
		}
		return time;
	}

	private void displayALEBlogContinuous ()
	{
		Date time = new Date();

		times = 0;
		sinit = 0;
		sloop1 = 0;
		snative = 0;
		sesper = 0;
		sfmc = 0;
		supdate = 0;
		ssave = 0;
		sseries = 0;
		sparent = 0;
		stotal = 0;
		sthings = 0;
		sfields = 0;
		System.out.println( "press 'ctr-c' to exit");

		displayALEBlogHeader();

		while ( true ) {
			time = displayALEBlogRecords(time);
			cu.sleep( 950 );
		}

	}

	private void displayALEBlog ()
	{
		Date time = new Date( new Date().getTime() - 1001000*60*10);

		System.out.println( "log from past 10 minutes ( " + time.toString() + ")");
		displayALEBlogHeader();

		time = displayALEBlogRecords(time);
	}

	public void execute() {
		setup();
		HashMap<String, String> options = new LinkedHashMap<String,String>();
		options.put("1", "ALEB log ");
		options.put("2", "ALEB log Continuos");

		Integer option = 0;
		while (option != null) {
			option = cu.showMenu("blink options", options );
			if (option != null) {
				if (option == 0) {
					displayALEBlog();
				}
				if (option == 1) {
					displayALEBlogContinuous();
				}

				System.out.println(cu.black() +  "\npress [enter] to continue");
				Scanner in = new Scanner(System.in);
				in.nextLine();
			}
		}

	}


}
