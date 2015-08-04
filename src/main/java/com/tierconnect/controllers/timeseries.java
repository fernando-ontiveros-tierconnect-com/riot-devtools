package com.tierconnect.controllers;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteOperation;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.tierconnect.dev.controllerInterface;
import com.tierconnect.utils.CommonUtils;
import com.tierconnect.utils.MqttUtils;
import com.tierconnect.utils.TimerUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;

/**
 * Created by fernando on 7/31/15.
 */
public class timeseries  implements controllerInterface
{

	CommonUtils cu;

	DBCollection timeseriesCollection;
	DBCollection outputCollection;

	MqttUtils mq;

	//local variables
	String groupId     = "3";
	String thingTypeId = "6,7,8";
	String fieldName   = "zone, brand, location, locationXYZ";
	String strYear   = "2015";
	String strMonth  = "7"; //zero based
	String strDay    = "1";
	String strHour   = "8";
	String strMinute = "0";
	String strSecond = "0";

	List<Long> arrayGroupId = new ArrayList<Long>();
	List<Long> arrayThingTypeId = new ArrayList<Long>();
	List<String> arrayFieldnames = new ArrayList<String>();

	public void setCu(CommonUtils cu) {
		this.cu = cu;
	}


	public String getDescription() {

		return "timeseries report";
	}

	public void setup()
	{
		timeseriesCollection        = cu.db.getCollection("timeseries");

		mq = new MqttUtils( "localhost", 1883);

	}

	private String read( String fname ) throws IOException
	{
		InputStream is = mapreduce.class.getResourceAsStream( fname );
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

	private void executeTimeserieReport()
	{
		outputCollection = cu.db.getCollection("report");
		outputCollection.drop();

		arrayGroupId.add( 3L );
		arrayThingTypeId.add( 6L );
		arrayThingTypeId.add( 8L );
		arrayThingTypeId.add( 7L );
		arrayFieldnames.add("zone");
		arrayFieldnames.add("location");
		arrayFieldnames.add("locationXYZ");
		arrayFieldnames.add("brand");

		int year   = Integer.parseInt(strYear);
		int month  = Integer.parseInt(strMonth);
		int day    = Integer.parseInt(strDay);
		int hour   = Integer.parseInt(strHour);
		int minute = Integer.parseInt(strMinute);
		int second = Integer.parseInt(strSecond);

		Calendar c = Calendar.getInstance();
		c.set(year, month, day, hour, minute, second);

		TimerUtils tu = new TimerUtils();
		Integer bulkOperations = 0;
		Long totalDocs = 0L;
		tu.mark();

		Long reportDate = c.getTime().getTime();

		BasicDBObject query = new BasicDBObject( "groupId", new BasicDBObject( "$in", arrayGroupId ) )
				.append( "thingTypeId", new BasicDBObject( "$in", arrayThingTypeId ) )
				.append( "fieldName",   new BasicDBObject( "$in", arrayFieldnames ) )
				.append( "prevEnd",     new BasicDBObject( "$lt", reportDate ) )
				.append( "nextStart",   new BasicDBObject( "$gt", reportDate ) );

		DBCursor cursor = timeseriesCollection.find(query);
		BulkWriteOperation bulkWriteOperation = outputCollection.initializeUnorderedBulkOperation();


		String prevSerialNumber = "";
		BasicDBObject newDoc = null;

		Long numDocs = 0L;
		while (cursor.hasNext()) {
			DBObject timeserie = cursor.next();
			numDocs ++;

			//var str = [];
			String value = null;
			Long   timeLastChange  = null;
			Integer i;
			BasicDBList timeList  = (BasicDBList) timeserie.get("time");
			BasicDBList valueList = (BasicDBList) timeserie.get("value");

			//go over the internal array to get the latest value for the date specified
			i = timeList.size()-1;
			while ( i >= 0) {
				if ( timeList.get(i) != null && ! timeList.get(i).equals( 0 ) ) {
					if (value == null ) {
						value = valueList.get(i).toString();
						timeLastChange  = Long.parseLong( timeList.get(i).toString());
					}
					if (timeLastChange > reportDate ) {
						i = -1;  //exit from this loop
					}
				}
				i--;
			}

			// build the document to be stored in the output collection
			if (!prevSerialNumber.equals( timeserie.get("serialNumber")) ) {
				if (newDoc != null) {
					bulkWriteOperation.insert( newDoc );
					bulkOperations ++;
					totalDocs ++;
					if (bulkOperations >= 1000) {
						bulkWriteOperation.execute();
						bulkWriteOperation = outputCollection.initializeUnorderedBulkOperation();
						bulkOperations = 0;
						System.out.println( totalDocs + " docs"  );
					}
				}
				BasicDBObject mongoId = (BasicDBObject)timeserie.get("_id");
				newDoc = new BasicDBObject()
						.append( "id",               mongoId.get( "id" ) )
						.append( "thingTypeFieldId", mongoId.get( "thingTypeFieldId" ) )
						.append( "thingTypeId",      timeserie.get( "thingTypeId" ) )
						.append( "serialNumber",     timeserie.get( "serialNumber" ) );
			}
			//add the fieldName with his value and with his date
			newDoc.append( timeserie.get("fieldName").toString(), value );

			newDoc.append( timeserie.get("fieldName").toString() + "Date", timeLastChange );
			newDoc.append( timeserie.get("fieldName").toString() + "Date2", new Date(timeLastChange) );

			prevSerialNumber = timeserie.get("serialNumber").toString();

		}
		if (newDoc != null) {
			bulkWriteOperation.insert( newDoc );
			bulkOperations ++;
			totalDocs ++;
		}
		if (bulkOperations > 0) {
			bulkWriteOperation.execute();
		}
		tu.mark();
		System.out.println( totalDocs + " docs"  );
		System.out.println("timeseries report generated in " + tu.getLastDelt() + " ms ");

	}

	private void timeserieReport()
	{
		Scanner in;
		in = new Scanner(System.in);
		Long reportDate = new Date().getTime();
		String groupId;
		String thingTypeId;
		String fieldName;
		String prevEnd;
		String nextStart;

		executeTimeserieReport();
		/*
		System.out.print(cu.ANSI_BLACK + "\nHow many things wants to change?[" + cu.ANSI_GREEN + "1000" + cu.ANSI_BLACK + "]:");
		String tagIn = in.nextLine();
		if (tagIn.equals("")) {
			tagIn = "1000";
		} else {
			tagIn = "" + Long.parseLong(tagIn);
		}
		thingsToChange = Long.parseLong(tagIn);

		System.out.print(cu.ANSI_BLACK + "\nHow many miliseconds (ms) between each blink ?[" + cu.ANSI_GREEN + delayBetweenThings + cu.ANSI_BLACK + "]:");
		tagIn = in.nextLine();
		if (tagIn.equals("")) {
			//delayBetweenThings = delayBetweenThings;
		} else {
			delayBetweenThings = Integer.parseInt( tagIn );
		}

		System.out.print(cu.ANSI_BLACK + "\nChanging " + thingsToChange + " things with a delay of " + delayBetweenThings + " ms.");

		*/
	}

	public void execute() {
		setup();
		HashMap<String, String> options = new HashMap<String,String>();

		options.put("1", "simple timeserie report");

		Integer option = 0;
		while (option != null) {
			option = cu.showMenu("blink options", options );
			if (option != null) {
				if (option == 0) {
					timeserieReport();
				}

				System.out.println(cu.ANSI_BLACK +  "\npress [enter] to continue");
				Scanner in = new Scanner(System.in);
				in.nextLine();
			}
		}

	}

}
