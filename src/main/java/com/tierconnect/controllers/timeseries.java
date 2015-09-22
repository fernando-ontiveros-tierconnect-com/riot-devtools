package com.tierconnect.controllers;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteOperation;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;
import com.tierconnect.dev.controllerInterface;
import com.tierconnect.utils.CommonUtils;
import com.tierconnect.utils.MqttUtils;
import com.tierconnect.utils.TimerUtils;
import de.undercouch.bson4jackson.BsonFactory;
import de.undercouch.bson4jackson.BsonGenerator;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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

	int  TIMESERIES_PER_SEGMENT = 100;

	//local variables
	String groupId     = "3";
	String thingTypeId = "6,7,8";
	String fieldName   = "zone, brand, location, locationXYZ";
	String strYear   = "2015";
	String strMonth  = "8"; //zero based
	String strDay    = "19";
	String strHour   = "12";
	String strMinute = "0";
	String strSecond = "0";
	String outputReport = "report";
	String outputReportSparse = "reportSparse";
	String lastSerialNumber = "000000000000000010001";

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

	private void executeDenseTimeserieReport(String outputReport, Date dateReport)
	{
		outputCollection = cu.db.getCollection(outputReport);
		outputCollection.drop();

		arrayGroupId.add( 3L );
		arrayThingTypeId.add( 6L );
		arrayThingTypeId.add( 8L );
		arrayThingTypeId.add( 7L );
		arrayFieldnames.add("zone");
		arrayFieldnames.add("location");
		arrayFieldnames.add("locationXYZ");
		arrayFieldnames.add("brand");

		TimerUtils tu = new TimerUtils();
		Integer bulkOperations = 0;
		Long totalDocs = 0L;
		tu.mark();

		System.out.println("Generate Report for " + dateReport );

		//BasicDBObject query = new BasicDBObject( "groupId", new BasicDBObject( "$in", arrayGroupId ) )
		//		.append( "thingTypeId", new BasicDBObject( "$in", arrayThingTypeId ) )
		//		.append( "fieldName",   new BasicDBObject( "$in", arrayFieldnames ) )
		//		.append( "prevEnd",     new BasicDBObject( "$lt", reportDate ) )
		//		.append( "nextStart",   new BasicDBObject( "$gt", reportDate ) );

		BasicDBObject query = new BasicDBObject()
				.append( "prevEnd",     new BasicDBObject( "$lt", dateReport ) )
				.append( "nextStart",   new BasicDBObject( "$gt", dateReport ) );

		DBCursor cursor = timeseriesCollection.find(query);
		tu.mark();
		System.out.println("QUERY: { prevEnd: {$lt: " + dateReport + "}, nextStart: {$gt: " + dateReport + "} }");

		BulkWriteOperation bulkWriteOperation = outputCollection.initializeUnorderedBulkOperation();


		String prevSerialNumber = "";
		BasicDBObject newDoc = null;

		Long numDocs = 0L;
		while (cursor.hasNext()) {
			DBObject timeserie = cursor.next();
			numDocs ++;

			//var str = [];
			DBObject value = null;
			Date   timeLastChange  = null;
			Integer i;
			BasicDBList timeList  = (BasicDBList) timeserie.get("time");
			BasicDBList valueList = (BasicDBList) timeserie.get("value");

			//go over the internal array to get the latest value for the date specified
			i = timeList.size()-1;
			while ( i >= 0) {
				if ( timeList.get(i) != null && ! timeList.get(i).equals( 0 ) ) {
					if (value == null ) {
						value = (DBObject)valueList.get(i);
						timeLastChange  = (Date) timeList.get( i );
					}
					if (timeLastChange.getTime() > dateReport.getTime() ) {
						i = -1;  //exit from this loop
					}
				}
				i--;
			}

			BasicDBObject mongoId = (BasicDBObject)timeserie.get("_id");
			newDoc = new BasicDBObject()
					.append( "_id", mongoId.get("id") )
					.append( "serialNumber",     timeserie.get( "serialNumber" ) )
					.append( "fields",    	   value );

			bulkWriteOperation.insert( newDoc );
			bulkOperations ++;
			totalDocs ++;
			if (bulkOperations >= 1000) {
				bulkWriteOperation.execute();
				bulkWriteOperation = outputCollection.initializeUnorderedBulkOperation();
				bulkOperations = 0;
				if (totalDocs % 10000L == 0)
				{
					System.out.println( totalDocs + " docs" );
				}
			}

			/*
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
						if (totalDocs % 10000L == 0)
						{
							System.out.println( totalDocs + " docs" );
						}
					}
				}
				BasicDBObject mongoId = (BasicDBObject)timeserie.get("_id");
				newDoc = new BasicDBObject()
						.append( "_id", mongoId.get( "id" ) )
						.append( "serialNumber",     timeserie.get( "serialNumber" ) );
			}
			//add the fieldName with his value and with his date

			//newDoc.append( timeserie.get( "fieldName" ).toString(), value );
			//newDoc.append( timeserie.get( "fieldName" ).toString() + "Date", timeLastChange );

			newDoc.append( "fields", value );

			prevSerialNumber = timeserie.get("serialNumber").toString();
			*/
		}
		/*
		if (newDoc != null) {
			bulkWriteOperation.insert( newDoc );
			bulkOperations ++;
			totalDocs ++;
		}
		if (bulkOperations > 0) {
			bulkWriteOperation.execute();
		}
		*/
		tu.mark();
		System.out.println( totalDocs + " docs"  );
		System.out.println("timeseries report generated in " + tu.getLastDelt() + " ms ");

	}

	private void executeSparseTimeserieReport(String outputReport, Date dateReport)
	{
		outputCollection = cu.db.getCollection(outputReport);
		DBCollection sparseCollection = cu.db.getCollection("sparse");

		outputCollection.drop();

		arrayGroupId.add( 3L );
		arrayThingTypeId.add( 6L );
		arrayThingTypeId.add( 8L );
		arrayThingTypeId.add( 7L );
		arrayFieldnames.add("zone");
		arrayFieldnames.add("location");
		arrayFieldnames.add("locationXYZ");
		arrayFieldnames.add("brand");

		TimerUtils tu = new TimerUtils();
		Integer bulkOperations = 0;
		Long totalDocs = 0L;
		tu.mark();

		System.out.println("Generate Report for " + dateReport );

		//BasicDBObject query = new BasicDBObject( "groupId", new BasicDBObject( "$in", arrayGroupId ) )
		//		.append( "thingTypeId", new BasicDBObject( "$in", arrayThingTypeId ) )
		//		.append( "fieldName",   new BasicDBObject( "$in", arrayFieldnames ) )
		//		.append( "prevEnd",     new BasicDBObject( "$lt", reportDate ) )
		//		.append( "nextStart",   new BasicDBObject( "$gt", reportDate ) );

		BasicDBObject query = new BasicDBObject()
				.append( "prevEnd",     new BasicDBObject( "$lt", dateReport ) )
				.append( "nextStart",   new BasicDBObject( "$gt", dateReport ) );

		DBCursor cursor = sparseCollection.find(query);
		tu.mark();
		System.out.println("QUERY: { prevEnd: {$lt: " + dateReport + "}, nextStart: {$gt: " + dateReport + "} }");

		BulkWriteOperation bulkWriteOperation = outputCollection.initializeUnorderedBulkOperation();

		String prevSerialNumber = "";
		BasicDBObject newDoc = null;

		Long numDocs = 0L;
		while (cursor.hasNext()) {
			DBObject timeserie = cursor.next();
			numDocs ++;

			//var str = [];
			DBObject value = null;
			Date   timeLastChange  = null;
			Integer i;
			BasicDBList timeList  = (BasicDBList) timeserie.get("time");
			BasicDBList valueList = (BasicDBList) timeserie.get("values");

			//go over the internal array to get the latest value for the date specified
			i = timeList.size()-1;
			while ( i >= 0) {
				if ( timeList.get(i) != null && ! timeList.get(i).equals( 0 ) ) {
					if (value == null ) {
						value = (DBObject)valueList.get(i);
						timeLastChange  = (Date) timeList.get( i );
					}
					if (timeLastChange.getTime() > dateReport.getTime() ) {
						i = -1;  //exit from this loop
					}
				}
				i--;
			}

			BasicDBObject mongoId = (BasicDBObject)timeserie.get("_id");
			newDoc = new BasicDBObject()
					.append( "_id", mongoId.get("id") )
					.append( "serialNumber",     timeserie.get( "serialNumber" ) )
					.append( "fields",    	   value );

			bulkWriteOperation.insert( newDoc );
			bulkOperations ++;
			totalDocs ++;
			if (bulkOperations >= 2000) {
				bulkWriteOperation.execute();
				bulkWriteOperation = outputCollection.initializeUnorderedBulkOperation();
				bulkOperations = 0;
				if (totalDocs % 10000L == 0)
				{
					System.out.println( totalDocs + " docs" );
				}
			}

			/*
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
						if (totalDocs % 10000L == 0)
						{
							System.out.println( totalDocs + " docs" );
						}
					}
				}
				BasicDBObject mongoId = (BasicDBObject)timeserie.get("_id");
				newDoc = new BasicDBObject()
						.append( "_id", mongoId.get( "id" ) )
						.append( "serialNumber",     timeserie.get( "serialNumber" ) );
			}
			//add the fieldName with his value and with his date

			//newDoc.append( timeserie.get( "fieldName" ).toString(), value );
			//newDoc.append( timeserie.get( "fieldName" ).toString() + "Date", timeLastChange );

			newDoc.append( "fields", value );

			prevSerialNumber = timeserie.get("serialNumber").toString();
			*/
		}
		/*
		if (newDoc != null) {
			bulkWriteOperation.insert( newDoc );
			bulkOperations ++;
			totalDocs ++;
		}
		if (bulkOperations > 0) {
			bulkWriteOperation.execute();
		}
		*/
		tu.mark();
		System.out.println( totalDocs + " docs"  );
		System.out.println("timeseries report generated in " + tu.getLastDelt() + " ms ");

	}

	private void denseTimeserieReport()
	{
		Integer year, month, day, hour, minute, second;
		do
		{
			year = Integer.valueOf( cu.prompt( "enter a Year ", "" + strYear ) );
		} while (year < 2015 || year > 2015);

		do
		{
			month = Integer.valueOf( cu.prompt( "enter a Month ", "" + strMonth ) );
		} while (month < 0 || month > 12);

		do
		{
			day = Integer.valueOf( cu.prompt( "enter a Day ", "" + strDay ) );
		} while (day < 1 || day > 31);

		do
		{
			hour = Integer.valueOf( cu.prompt( "enter a Hour ", "" + strHour ) );
		} while (hour < 0 || hour > 24);

		do
		{
			minute = Integer.valueOf( cu.prompt( "enter a Minute ", "" + strMinute ) );
		} while (minute < 0 || minute > 60);

		//do
		//{
		//	second = Integer.valueOf( cu.prompt( "enter a Second ", "" + strSecond ) );
		//} while (second < 0 || second > 60);
		second = Integer.valueOf( strSecond ) ;

		Calendar c = Calendar.getInstance();
		c.set(year, month -1, day, hour, minute, second);
		Date dateReport = c.getTime();

		outputReport = cu.prompt( "enter Output Report Collection ", "" + outputReport );
		executeDenseTimeserieReport( outputReport, dateReport );


	}

	private void sparseTimeserieReport()
	{
		Integer year, month, day, hour, minute, second;
		do
		{
			year = Integer.valueOf( cu.prompt( "enter a Year ", "" + strYear ) );
		} while (year < 2015 || year > 2015);

		do
		{
			month = Integer.valueOf( cu.prompt( "enter a Month ", "" + strMonth ) );
		} while (month < 0 || month > 12);

		do
		{
			day = Integer.valueOf( cu.prompt( "enter a Day ", "" + strDay ) );
		} while (day < 1 || day > 31);

		do
		{
			hour = Integer.valueOf( cu.prompt( "enter a Hour ", "" + strHour ) );
		} while (hour < 0 || hour > 24);

		do
		{
			minute = Integer.valueOf( cu.prompt( "enter a Minute ", "" + strMinute ) );
		} while (minute < 0 || minute > 60);

		//do
		//{
		//	second = Integer.valueOf( cu.prompt( "enter a Second ", "" + strSecond ) );
		//} while (second < 0 || second > 60);
		second = Integer.valueOf( strSecond ) ;

		Calendar c = Calendar.getInstance();
		c.set(year, month -1, day, hour, minute, second);
		Date dateReport = c.getTime();

		outputReport = cu.prompt( "enter Output Report Collection ", "" + outputReportSparse );
		executeSparseTimeserieReport( outputReport, dateReport );
	}

	public void bsonExamples()
	{

		String serialNumber = "000000000000000000000" + cu.prompt ( "enter a serialNumber ", lastSerialNumber);
    	serialNumber = serialNumber.substring(serialNumber.length()-21, serialNumber.length());

		DBObject prevThing = cu.getThing(serialNumber);
		cu.displayThing(prevThing);

		//serialize data
		ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);

		BsonFactory factory = new BsonFactory();
		factory.enable( BsonGenerator.Feature.ENABLE_STREAMING);

		ObjectMapper mapper = new ObjectMapper(factory);
		try {
			mapper.writeValue(baos, prevThing);
		} catch( IOException e ) {
			System.out.println ( e );
		}

		//deserialize data
		Map<String,Object> clone_of_thing = null;
		BasicDBObject clone = null;
		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		try
		{
			clone  = mapper.readValue(baos.toByteArray(), BasicDBObject.class);
		} catch( JsonParseException e ) {
			System.out.println(e);
		} catch( JsonMappingException e ) {
			System.out.println(e);
		} catch( IOException e ) {
			System.out.println(e);
		}

		Iterator<Map.Entry<String,Object>> it = clone.entrySet().iterator();
		BasicDBObject cloned = new BasicDBObject(  );
		while (it.hasNext()) {
			Map.Entry<String, Object> pair = it.next();

			if ( pair.getValue().getClass().toString().equals( "class java.util.LinkedHashMap")) {
				BasicDBObject field = new BasicDBObject(  );
				Map<String, Object> pairValue = (Map<String, Object>)pair.getValue();

				Iterator<Map.Entry<String,Object>> itField = pairValue.entrySet().iterator();
				while (itField.hasNext()) {
					Map.Entry<String, Object> pairField = itField.next();

					System.out.println( pairField.getValue().getClass().toString());

					//a hack to allow time field convert to Date
					if ( pairField.getValue().getClass().toString().equals( "class java.lang.Long") &&
							pairField.getKey().toString().equals("time")) {
						field.append( pairField.getKey(), new Date( Long.valueOf( pairField.getValue().toString() )) );
					} else
					{
						field.append( pairField.getKey(), pairField.getValue() );
					}
				}
				cloned.append( pair.getKey(), field );

			} else
			{
				cloned.append( pair.getKey(), pair.getValue() );
			}

		}

		System.out.print( "cloned:" );
        cu.displayThing( cloned );

		System.out.print( "diff:" );
		cu.diffThings( cloned, prevThing);

	}

	private Boolean getSegmentAndPointer( HashMap<String, Object> history )
	{
		HashMap<String, Object> res = new HashMap<>();

		//init the segment and pointer to null values before the process
		history.put("segment", null);
		history.put("pointer", 0L);

		//get the thing from the temporal hashmap
		DBObject thing = (DBObject)history.get("thing");
		DBCollection control = (DBCollection)history.get("timeseriesControlCollection");

		//the primary key has two values
		BasicDBObject key = new BasicDBObject( "id", history.get("thingId") )
				.append( "thingTypeFieldId",    history.get("thingTypeFieldId") );

		//first call to Database
		//probably we can improve it, and dont query the DB, if this data is part of thing cache
		BasicDBObject query = new BasicDBObject( "_id", key );

		try
		{
			DBObject doc = control.findOne( query );
			if ( doc != null )
			{
				//save the segment, for later use, when change the nextStart for a previous timeseries doc
				if (doc.containsField( "segment" ) && doc.get( "segment" ) != null )
				{
					history.put( "segment", doc.get( "segment" ) );
				}
				if (doc.containsField( "lastDate" ) && doc.get( "lastDate" ) != null )
				{
					history.put( "lastDate", doc.get( "lastDate" ) );
				}

				//since we have access to the lastValue and lastDate in the database,
				//here we are checking if the timestamp is different to save the timeseries
				if (doc.containsField( "lastValue" ) && doc.containsField( "lastDate" ))
				{
					//if ( (doc.get("lastDate").toString().equals( history.get("timestamp").toString() )) {
					if ( (doc.get("lastDate") == history.get("timestamp") )) {
						//return before, because the field does not change
						return false;
					}
				}

				//update the control collection, if the segment is full create a new one
				decrementSegment( history );
			}
			else
			{
				//if the segment doesn't exists, we need to create it
				// in history map, we already have the segment and the pointer
				// the second parameters, means we dont have a previous date because is first time
				createSegment( history, null);
			}
		}
		catch( Exception e )
		{
			System.out.println( e );
		}
		return true;
	}

	private void createSegment( HashMap<String, Object> history, Date prevEnd )
	{
		//two calls to Database to create a new timeseries document, and the timeseriesControl
		//if this is the first time for a udf, the timeseriesControl is created, in other case is updated
		DBObject thing             = (DBObject)history.get("thing");
		DBCollection control    = (DBCollection)history.get("timeseriesControlCollection");
		DBCollection timeseries = (DBCollection)history.get("timeseriesCollection");
		//Long thingTypeFieldId   = (Long)history.get("thingTypeFieldId");
		String thingTypeFieldId   = (String)history.get("thingTypeFieldId");
		String fieldName        = (String)history.get("fieldName");
		String value            = (String)history.get("value");
		Date timestamp          = (Date)history.get("timestamp");
		Long segment            = new Date().getTime();

		//the primary key has two values
		BasicDBObject key = new BasicDBObject( "id", history.get( "thingId" ) ).append( "thingTypeFieldId",    thingTypeFieldId );

		//if prevEnd is null, create the 'record' for this udf
		if (prevEnd == null)
		{
			//create the control collection, only if the collection does not exist previously
			DBObject doc = new BasicDBObject( "_id", key )
					//.append( "thingTypeId", thing.getThingType().id )
					//.append( "groupId", thing.getGroupId() )
					//.append( "name", thing.getName() )
					.append( "serialNumber", history.get("serialNumber") )
					.append( "fieldName", fieldName )

					.append( "lastDate", timestamp )
					.append( "lastValue", value )
					.append( "segment", segment )
					.append( "pointer", this.TIMESERIES_PER_SEGMENT );

			control.insert( doc );
		} else {
			//update the segment with the new one, and pointer to the max timeseries per segment
			DBObject query = new BasicDBObject( "_id", key );
			DBObject updateDoc = new BasicDBObject( "segment", segment )
					.append( "pointer", this.TIMESERIES_PER_SEGMENT );
			DBObject setDoc = new BasicDBObject( "$set", updateDoc );
			control.setWriteConcern( WriteConcern.UNACKNOWLEDGED  );

			DBObject doc = control.findAndModify( query, null, null, false, setDoc, true, false );
		}

		//fill two arrays with empty values
		ArrayList<Long> arrayTime  = new ArrayList<>( this.TIMESERIES_PER_SEGMENT );
		ArrayList<String> arrayValue = new ArrayList<>( this.TIMESERIES_PER_SEGMENT );
		for (int i = 0; i <= this.TIMESERIES_PER_SEGMENT; i++) {
			arrayTime.add(i, 0L);
			arrayValue.add(i, "");
		}

		//if prevEnd is null, is because this is the first time for this udf for this thing id
		if (prevEnd == null)
		{
			prevEnd = timestamp;
		}
		BasicDBObject keyTimeseries = new BasicDBObject( "id", history.get( "thingId" ) )
				.append( "thingTypeFieldId",    history.get("thingTypeFieldId") )
				.append( "segment", segment );

		DBObject timedoc = new BasicDBObject( "_id", keyTimeseries)
				//.append( "thingTypeId",  thing.getThingType().id )
				//.append( "groupId",      thing.getGroupId() )
				//.append( "name",         thing.getName() )
				.append( "serialNumber", history.get( "serialNumber" ) )
				.append( "fieldName", fieldName )

				.append( "prevEnd", prevEnd )
				//.append( "nextStart", Long.MAX_VALUE )
				.append( "nextStart", new Date(Long.MAX_VALUE) )
				.append( "time", arrayTime )
				.append( "value", arrayValue );

		timeseries.insert( timedoc );

		//update the nextStart when we are creating a new timeseries document
		if (prevEnd != null && history.get("segment") != null ) {
			if ( !history.get("segment").toString().equals( segment.toString() ))
			{
				BasicDBObject keyOldTimeseries = new BasicDBObject( "id", history.get( "thingId" ) )
						.append( "thingTypeFieldId", history.get( "thingTypeFieldId" ) )
						.append( "segment", history.get( "segment" ) );
				DBObject query = new BasicDBObject( "_id", keyOldTimeseries );
				DBObject updateDoc = new BasicDBObject( "nextStart", timestamp );
				DBObject setDoc = new BasicDBObject( "$set", updateDoc );
				timeseries.findAndModify( query, null, null, false, setDoc, true, false );
			}
		}

		history.put ("segment", segment);
		history.put ("pointer", this.TIMESERIES_PER_SEGMENT);

	}


	private void decrementSegment( HashMap<String, Object> history )
	{
		DBObject thing             = (DBObject)history.get("thing");
		DBCollection control    = (DBCollection)history.get("timeseriesControlCollection");
		DBCollection timeseries = (DBCollection)history.get("timeseriesCollection");
		String thingTypeFieldId   = (String)history.get("thingTypeFieldId");
		//Long thingTypeFieldId   = (Long)history.get("thingTypeFieldId");
		String fieldName        = (String)history.get("fieldName");
		String value            = (String)history.get("value");
		Date timestamp          = (Date)history.get("timestamp");
		Long segment            = new Date().getTime();

		//the primary key has two values
		BasicDBObject key = new BasicDBObject( "id", history.get("thingId") )
				.append( "thingTypeFieldId",    thingTypeFieldId );

		DBObject query = new BasicDBObject( "_id", key );

		DBObject setDoc = new BasicDBObject( "lastDate", timestamp )
				.append( "lastValue", value );

		DBObject incDoc = new BasicDBObject( "pointer", -1 );

		DBObject updateDoc = new BasicDBObject( "$set", setDoc )
				.append( "$inc", incDoc );

		DBObject doc = control.findAndModify( query, null, null, false, updateDoc, true, false );
		Integer pointer = Integer.parseInt( doc.get("pointer").toString() );
		if (pointer < 0 ) {
			if (history.containsKey( "lastDate" ) && history.get("lastDate") != null)
			{
				createSegment( history, (Date)history.get( "lastDate" ) );
			}
			else
			{
				createSegment( history, new Date() );
			}
		}
		else
		{
			history.put( "segment", doc.get( "segment" ) );
			history.put( "pointer", doc.get( "pointer" ) );
		}
	}

	private void updateTimeseriesSegment( HashMap<String, Object> history )
	{

		//here we asuming the "segment" and the "pointer" have been calculated previously
		//and this method is updating the timeseries document in Database
		DBObject thing             = (DBObject)history.get("thing");
		DBCollection timeseries = (DBCollection)history.get("timeseriesCollection");
		String value            = (String)history.get("value");
		Date   timestamp        = (Date)history.get("timestamp");
		Long segment            = (Long)history.get("segment");
		Integer pointer         = (Integer)history.get("pointer");

		try {
			BasicDBObject key = new BasicDBObject( "id", history.get("thingId") )
					.append( "thingTypeFieldId",    history.get("thingTypeFieldId") )
					.append( "segment",    segment );

			BasicDBObject query = new BasicDBObject( "_id", key);

			DBObject updateDoc = new BasicDBObject( "time." + pointer, timestamp )
					.append( "value." + pointer, value );

			timeseries.setWriteConcern( WriteConcern.UNACKNOWLEDGED  );
			timeseries.update( query, new BasicDBObject( "$set", updateDoc ));
		}
		finally
		{
		}
	}

	public void insertThingHistory( DBObject thing)
	{
		//hash map to store fields needed to update both Collections, timeseries and timeseriesControl
		HashMap<String, Object> history = new HashMap<>();
		try {
			DBCollection timeseriesCollection        = cu.db.getCollection( "timeseries" );
			DBCollection timeseriesControlCollection = cu.db.getCollection( "timeseriesControl" );

			history.put("timeseriesCollection",        timeseriesCollection);
			history.put("timeseriesControlCollection", timeseriesControlCollection);
			history.put("thing", thing);

			DBObject mongoId = (DBObject)thing.get("_id");
			String fieldName = mongoId.get("thingTypeFieldId").toString();
			history.put("fieldName", fieldName );

			DBObject mrValue = (DBObject)thing.get("value");
			DBObject field   = (DBObject)mrValue.get(fieldName);

			history.put("thingId", mongoId.get("id") );
			history.put("serialNumber", mrValue.get("serialNumber") );

			//temporaly use the fieldname instead the thingTypeFieldId
			//history.put("thingTypeFieldId", field.get("thingTypeFieldId") );
			history.put("thingTypeFieldId", fieldName );

			String value     = field.get("value").toString();
			Date   timestamp = (Date)field.get("time");

			history.put("value", value);
			history.put("timestamp", timestamp);

			//update the TimeseriesControl collection to get the current segment and pointer
			//but first check if the udf has changed, if not continue
			if ( getSegmentAndPointer( history ) )
			{
				//update the segment with the last value
				updateTimeseriesSegment( history );
			}
		}
		catch( MongoException me )
		{
			System.out.println( me );
		}
		//System.out.println( "[Insert timeseries in timeseries by segment] " + history.get( "serialNumber" ) + "[" + history.get("thingId") + "]" );
	}



	public void importTimeseries()
	{

		String collectionName = cu.prompt( "enter the source DBcollection ", "demo4" );

		DBCollection timeseries        = cu.db.getCollection("timeseries");
		DBCollection timeseriescontrol = cu.db.getCollection("timeseriesControl");
		timeseries.drop();
		timeseriescontrol.drop();

		DBCollection tempCollection = cu.db.getCollection(collectionName);

		DBCursor cursor = tempCollection.find();

		Long times = 0L;
		while (cursor.hasNext()) {
			DBObject tdoc = cursor.next();
			insertThingHistory( tdoc);
			times++;
			if ( times % 1000 == 0 )
			{
				System.out.println( "[Inserted " + times + " timeseries ] " + ((DBObject) tdoc.get( "_id" )).get( "id" ) );
			}

		}



	}

	public void execute() {
		setup();
		HashMap<String, String> options = new LinkedHashMap<String,String>();

		options.put("1", "Dense timeserie report");
		options.put("2", "Sparse timeserie report");
		options.put("3", "bson examples");
		options.put("4", "import timeseries from thingSnapshot temporal");

		Integer option = 0;
		while (option != null) {
			option = cu.showMenu("timeseries options", options );
			if (option != null) {
				if (option == 0) {
					denseTimeserieReport();
				}
				if (option == 1) {
					sparseTimeserieReport();
				}
				if (option == 2) {
					bsonExamples();
				}
				if (option == 3) {
					importTimeseries();
				}

				System.out.println(cu.black() +  "\npress [enter] to continue");
				Scanner in = new Scanner(System.in);
				in.nextLine();
			}
		}

	}

}
