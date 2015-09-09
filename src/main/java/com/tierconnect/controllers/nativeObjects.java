package com.tierconnect.controllers;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.tierconnect.dev.controllerInterface;
import com.tierconnect.utils.CommonUtils;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;

/**
 * Created by fernando on 7/17/15.
 */
public class nativeObjects implements controllerInterface
{
	CommonUtils cu;
	String lastSerialNumber = "000000000000000010000";

	Long sequenceNumber = 0L;
	Long serialNumber = 200L;

	DBCollection thingsCollection;
	DBCollection outputCollection;
	BasicDBObject docs[];
	String tag;

	String thingTypeCode = "default_gps_thingtype";
	String multipleThingTypeCode = "Native.Objects.Multiple";
	String defaultRfidThingTypeCode = "default_rfid_thingtype";
	String statusThingField = "status";
	String logicalReaderField = "logicalReader";
	String multipleLogicalReaderField = "multiLogicalReader";
	String zoneField = "zone";
	String multiplezoneField = "multiZone";
	String shiftField = "shift";
	String multipleshiftField = "multiShift";

	String thingFieldJSON = "shifts";

	ArrayList<HashMap<String,Object>> logicalReaders;
	ArrayList<HashMap<String,Object>> shifts;
	ArrayList<HashMap<String,Object>> zones;

	public void setCu( CommonUtils cu )
	{
		this.cu = cu;
	}

	public String getDescription()
	{

		return "native Objects";
	}

	public void setup()
	{
		thingsCollection = cu.db.getCollection( "things" );
		outputCollection = cu.db.getCollection( "mr_reusableTag" );

		cu.defaultMqttConnection();
	}

	public void createThingTypes() {
		try {
			cu.createThingTypeFromFile( "/nativeObjectsMultiple.txt" );
		} catch (Exception e) {
			System.out.println(e.getCause());
		}

		HashMap<String,Object> res;

		try {
			System.out.println( cu.blue() + "creating logical readers for tests" + cu.black());

			String body = cu.read( "/coreServices/logicalReader.txt" );

			JsonFactory factory = new JsonFactory();
			ObjectMapper mapper = new ObjectMapper(factory);
			TypeReference<HashMap<String,Object>> typeRef = new TypeReference<HashMap<String,Object>>() {};

			res = mapper.readValue(body, typeRef);

			ArrayList<HashMap<String, Object>> values = (ArrayList<HashMap<String, Object>>)res.get("values" );

			for ( int i = 0; i < values.size(); i++ )
			{
				Map<String,Object> map =  values.get( i );
				ObjectMapper mapper2 = new ObjectMapper();
				String jsonFromMap = mapper.writeValueAsString(map);
				System.out.println( "Got " + jsonFromMap );
				cu.httpPutMessage( "logicalReader", jsonFromMap);
			}

			System.out.println( cu.blue() + "creating Shifts for tests" + cu.black());

			body = cu.read( "/coreServices/shift.txt" );

			res = mapper.readValue(body, typeRef);

			values = (ArrayList<HashMap<String, Object>>)res.get("values" );

			for ( int i = 0; i < values.size(); i++ )
			{
				Map<String,Object> map =  values.get( i );
				String jsonFromMap = mapper.writeValueAsString(map);
				System.out.println( "Got " + jsonFromMap );
				cu.httpPutMessage( "shift", jsonFromMap);
			}
		} catch (Exception e) {
			System.out.println(e.getCause());
		}

	}

	private String castSerialNumber( Long n )
	{
		String serial = "000000000000000000000" + n;
		serial = serial.substring( serial.length() - 21, serial.length() );
		return serial;
	}

	private String nextSerialNumber()
	{
		serialNumber++;
		String serial = "000000000000000000000" + serialNumber;
		serial = serial.substring( serial.length() - 21, serial.length() );
		return serial;
	}

	private void sendChangeMessage( Integer sequenceNumber, String serialNumber, String thingType, Integer delayBetweenThings )
	{

		String topic = "/v1/data/ALEB/" + thingType;
		StringBuffer msg = new StringBuffer();
		msg.append( " sn," + sequenceNumber + "\n" );
		msg.append( ",0,___CS___,-118.443969;34.048092;0.0;20.0;ft\n" );
		Long time = new Date().getTime();
		Random r = new Random();

		if( thingType.equals( "forklift" ) )
		{
			msg.append( serialNumber + "," + time + ",lastDetectTime," + time + "\n" );
		}
		else
		{
			msg.append( serialNumber + "," + time + ",location,-118.44395517462448;34.04811656588989;0.0\n" );
			msg.append( serialNumber + "," + time + ",locationXYZ," + r.nextInt( 499 ) + ".0;" + r.nextInt( 499 ) + ".0;0.0\n" );
			msg.append( serialNumber + "," + time + ",logicalReader,LR" + r.nextInt( 10 ) + "\n" );
			msg.append( serialNumber + "," + time + ",lastDetectTime," + time + "\n" );
		}
		cu.publishSyncMessage( topic, msg.toString() );
		if( delayBetweenThings > 0 )
		{
			cu.sleep( delayBetweenThings );
		}
	}

	private void changeThings( Boolean allThings )
	{
		String tag;
		StringBuffer sb = new StringBuffer();
		Scanner in;
		in = new Scanner( System.in );
		Long thingsToChange = 0L;
		Integer delayBetweenThings = 10;

		System.out.print( cu.black() + "\nHow many things wants to change?[" + cu.green() + "1000" + cu.black() + "]:" );
		String tagIn = in.nextLine();
		if( tagIn.equals( "" ) )
		{
			tagIn = "1000";
		}
		else
		{
			tagIn = "" + Long.parseLong( tagIn );
		}
		thingsToChange = Long.parseLong( tagIn );

		System.out.print( cu.black() + "\nHow many miliseconds (ms) between each blink ?[" + cu.green() + delayBetweenThings + cu.black() + "]:" );
		tagIn = in.nextLine();
		if( tagIn.equals( "" ) )
		{
			//delayBetweenThings = delayBetweenThings;
		}
		else
		{
			delayBetweenThings = Integer.parseInt( tagIn );
		}

		System.out.print( cu.black() + "\nChanging " + thingsToChange + " things with a delay of " + delayBetweenThings + " ms." );

		//get the max number of _id
		Long maxId = 0L;
		String serialNumber;
		String thingType;
		Random random = new Random();

		DBObject sortby = new BasicDBObject( "_id", -1 );
		DBCursor cursor = thingsCollection.find().sort( sortby ).limit( 1 );
		try
		{
			if( cursor.hasNext() )
			{
				cursor.next();
				maxId = Long.parseLong( cursor.curr().get( "_id" ).toString() );
			}
		}
		finally
		{
			cursor.close();
		}
		if( maxId == 0 )
		{
			System.out.println( "error or zero documents in collection 'things' " );
			return;
		}

		System.out.println( "The max value for _id is " + maxId );

		for( Integer i = 0; i < thingsToChange; )
		{
			DBObject filterById = new BasicDBObject( "_id", random.nextLong() % maxId );
			cursor = thingsCollection.find( filterById ).limit( 1 );
			try
			{
				if( cursor.hasNext() )
				{
					cursor.next();
					serialNumber = cursor.curr().get( "serialNumber" ).toString();
					thingType = cursor.curr().get( "thingTypeCode" ).toString();
					if( allThings || thingType.equals( "forkliftBattery" ) || thingType.equals( "forkliftSolar" ) )
					{
						System.out.println( i + " " + serialNumber + " " + thingType );
						sendChangeMessage( i, serialNumber, thingType, delayBetweenThings );
						i++;
					}
				}
			}
			finally
			{
				cursor.close();
			}

		}

	}

	private void changeOneThing()
	{
		StringBuffer sb = new StringBuffer();
		Scanner in;
		in = new Scanner( System.in );
		Long thingsToChange = 0L;
		Integer delayBetweenThings = 10;

		System.out.print( cu.black() + "\nserialNumber[" + cu.green() + lastSerialNumber + cu.black() + "]:" );
		String tagIn = in.nextLine();
		if( tagIn.equals( "" ) )
		{
			tagIn = lastSerialNumber;
		}
		else
		{
			tagIn = "" + tagIn;
		}

		String serialNumber = castSerialNumber( Long.parseLong( tagIn ) );
		lastSerialNumber = serialNumber;

		System.out.print( cu.black() + "\nChanging the thing " + serialNumber );

		//get the max number of _id
		Long maxId = 0L;
		String thingType = "";

		DBObject query = new BasicDBObject( "serialNumber", serialNumber );
		DBCursor cursor = thingsCollection.find( query );
		try
		{
			if( cursor.hasNext() )
			{
				cursor.next();
				thingType = cursor.curr().get( "thingTypeCode" ).toString();
			}
		}
		finally
		{
			cursor.close();
		}
		if( thingType.equals( "" ) )
		{
			System.out.println( "error or zero documents in collection 'things' " );
			return;
		}

		sendChangeMessage( 1, serialNumber, thingType, 0 );

	}

	private void sendCommasBlink()
	{
		StringBuffer sb = new StringBuffer();

		Random r = new Random();
		String serialNumber = "";

		serialNumber = "000000000000000000000" + cu.prompt( "enter a serialNumber", lastSerialNumber );
		lastSerialNumber = serialNumber.substring( serialNumber.length() - 21, serialNumber.length() );
		serialNumber = lastSerialNumber;

		defaultRfidThingTypeCode = cu.prompt( "enter the thingTypeCode", defaultRfidThingTypeCode );

		statusThingField = cu.prompt( "enter the udf name", statusThingField );

		String fruits[] = { "apples", "oranges", "bananas", "grapes", "strawberries", "watermelon", "pineapples" };
		String commaValue = "";
		for( int i = 0; i < r.nextInt( 2 ) + 2; i++ )
		{
			commaValue += (commaValue.equals( "" ) ? "" : ", ") + (r.nextInt( 10 ) + 1) + " " + fruits[r.nextInt( fruits.length - 1 )];
		}

		String topic = "/v1/data/ALEB/" + defaultRfidThingTypeCode;

		sequenceNumber = cu.getSequenceNumber();
		Long time = new Date().getTime();
		sb.append( " sn," + sequenceNumber + "\n" );
		sb.append( ",0,___CS___,-118.443969;34.048092;0.0;20.0;ft\n" );

		sb.append( serialNumber + "," + time + "," + statusThingField + "," + "\"" + commaValue + "\"\n" );

		System.out.println( " serialNumber: " + cu.blue() + serialNumber + cu.black() + "" );
		System.out.println( "thingTypeCode: " + cu.blue() + defaultRfidThingTypeCode + cu.black() + "" );
		System.out.println( "        field: " + cu.blue() + statusThingField + cu.black() + "" );
		System.out.println( "        value: " + cu.blue() + commaValue + cu.black() + "" );

		DBObject prevThing = cu.getThing( serialNumber, defaultRfidThingTypeCode );

		cu.publishSyncMessage( topic, sb.toString() );
		cu.sleep( 1000 );

		DBObject newThing = cu.getThing( serialNumber, defaultRfidThingTypeCode );
		cu.diffThings( newThing, prevThing );
	}

	private String getRandomLR()
	{
		Random r = new Random();

		if (logicalReaders.size() == 0)
		{
			System.out.println("Error, the LogicalReaders list is empty!");
		}

		int index = r.nextInt( logicalReaders.size());

		HashMap<String,Object> entry = (HashMap<String,Object>) logicalReaders.get(index);
		if (r.nextDouble() < 0.5)
		{
			return  entry.get( "code" ).toString();
		} else {
			return entry.get( "id" ).toString();
		}

	}

	private String getMultipleRandomLR()
	{
		Random r = new Random();
		StringBuilder s = new StringBuilder( "[" );
		int n = r.nextInt( 3 ) +1;
		for (int i = 0; i < n; i++ ) {
			if (!s.toString().equals( "[" )) {
				s.append( ", " );
			}
			String strId = getRandomLR();
			String value = '"' + strId + '"';
			try
			{
				value = "" + Long.valueOf( strId);
			} catch( NumberFormatException e ) {
			}

			s.append( value );
		}
		s.append( "]" );
		return s.toString();
	}

	private void getLogicalReaders()
	{
		HashMap<String,Object> res;
		try
		{
			res = cu.httpGetMessage( "logicalReader?pageSize=100");

			logicalReaders = (ArrayList<HashMap<String,Object>> )res.get("results");
			//System.out.println( logicalReaders );

			System.out.println("Valid LogicalReaders:");
			Iterator it = logicalReaders.iterator();
			while (it.hasNext()) {
				HashMap<String,Object> entry = (HashMap<String,Object>)it.next();
				System.out.println( "    id:" + entry.get("id") + " code:" + entry.get("code") );
			}

		}
		catch( IOException e )
		{
			e.printStackTrace();
		}
		catch( URISyntaxException e )
		{
			e.printStackTrace();
		}

	}

	private void sendLogicalReaderBlink()
	{

		getLogicalReaders();
		StringBuffer sb = new StringBuffer();

		String serialNumber = "";

		serialNumber = "000000000000000000000" + cu.prompt( "enter a serialNumber", lastSerialNumber );
		lastSerialNumber = serialNumber.substring( serialNumber.length() - 21, serialNumber.length() );
		serialNumber = lastSerialNumber;

		defaultRfidThingTypeCode = cu.prompt( "enter the thingTypeCode", defaultRfidThingTypeCode );

		logicalReaderField = cu.prompt( "enter the udf name", logicalReaderField );

		String lrValue = getRandomLR();
		lrValue = cu.prompt( "enter the Logical Reader Value", lrValue );

		String topic = "/v1/data/ALEB/" + defaultRfidThingTypeCode;

		sequenceNumber = cu.getSequenceNumber();
		Long time = new Date().getTime();
		sb.append( " sn," + sequenceNumber + "\n" );
		sb.append( ",0,___CS___,-118.443969;34.048092;0.0;20.0;ft\n" );

		sb.append( serialNumber + "," + time + "," + logicalReaderField + "," + "\"" + lrValue + "\"\n" );

		System.out.println( " serialNumber: " + cu.blue() + serialNumber + cu.black() + "" );
		System.out.println( "thingTypeCode: " + cu.blue() + defaultRfidThingTypeCode + cu.black() + "" );
		System.out.println( "        field: " + cu.blue() + logicalReaderField + cu.black() + "" );
		System.out.println( "        value: " + cu.blue() + lrValue + cu.black() + "" );

		DBObject prevThing = cu.getThing( serialNumber, defaultRfidThingTypeCode );

		cu.publishSyncMessage( topic, sb.toString() );
		cu.sleep( 1000 );

		DBObject newThing = cu.getThing( serialNumber, defaultRfidThingTypeCode );
		cu.diffThings( newThing, prevThing );
	}

	private void sendMultipleLogicalReaderBlink()
	{

		getLogicalReaders();
		StringBuffer sb = new StringBuffer();

		String serialNumber = "";

		serialNumber = "000000000000000000000" + cu.prompt( "enter a serialNumber", lastSerialNumber );
		lastSerialNumber = serialNumber.substring( serialNumber.length() - 21, serialNumber.length() );
		serialNumber = lastSerialNumber;

		multipleThingTypeCode = cu.prompt( "enter the thingTypeCode", multipleThingTypeCode );

		multipleLogicalReaderField = cu.prompt( "enter the udf name", multipleLogicalReaderField );

		String lrValue = getMultipleRandomLR();
		lrValue = cu.prompt( "enter the Logical Reader Value", lrValue );

		String topic = "/v1/data/ALEB/" + multipleThingTypeCode;

		sequenceNumber = cu.getSequenceNumber();
		Long time = new Date().getTime();
		sb.append( " sn," + sequenceNumber + "\n" );
		sb.append( ",0,___CS___,-118.443969;34.048092;0.0;20.0;ft\n" );

		sb.append( serialNumber + "," + time + "," + multipleLogicalReaderField + "," + "\"" + lrValue + "\"\n" );

		System.out.println( " serialNumber: " + cu.blue() + serialNumber + cu.black() + "" );
		System.out.println( "thingTypeCode: " + cu.blue() + multipleThingTypeCode + cu.black() + "" );
		System.out.println( "        field: " + cu.blue() + multipleLogicalReaderField + cu.black() + "" );
		System.out.println( "        value: " + cu.blue() + lrValue + cu.black() + "" );

		DBObject prevThing = cu.getThing( serialNumber, multipleThingTypeCode );

		cu.publishSyncMessage( topic, sb.toString() );
		cu.sleep( 1000 );

		DBObject newThing = cu.getThing( serialNumber, multipleThingTypeCode );
		cu.diffThings( newThing, prevThing );
	}

	private String getRandomZone()
	{
		Random r = new Random();

		if (zones.size() == 0)
		{
			System.out.println("Error, the Zones list is empty!");
		}

		int index = r.nextInt( zones.size());

		HashMap<String,Object> entry = (HashMap<String,Object>) zones.get(index);
		if (r.nextDouble() < 0.5)
		{
			return  entry.get( "code" ).toString();
		} else {
			return entry.get( "id" ).toString();
		}

	}

	private String getMultipleRandomZone()
	{
		Random r = new Random();
		StringBuilder s = new StringBuilder( "[" );
		int n = r.nextInt( 3 ) +1;
		for (int i = 0; i < n; i++ ) {
			if (!s.toString().equals( "[" )) {
				s.append( ", " );
			}
			String strId = getRandomZone();
			String value = '"' + strId + '"';
			try
			{
				value = "" + Long.valueOf( strId);
			} catch( NumberFormatException e ) {
			}

			s.append( value );
		}
		s.append( "]" );
		return s.toString();
	}


	private void getZones()
	{
		HashMap<String,Object> res;
		try
		{
			res = cu.httpGetMessage( "zone");

			zones = (ArrayList<HashMap<String,Object>> )res.get("results");
			//System.out.println( zones );

			System.out.println("Valid Zones:");
			Iterator it = zones.iterator();
			while (it.hasNext()) {
				HashMap<String,Object> entry = (HashMap<String,Object>)it.next();
				System.out.println( "    id:" + entry.get("id") + " code:" + entry.get("code") );
			}

		}
		catch( IOException e )
		{
			e.printStackTrace();
		}
		catch( URISyntaxException e )
		{
			e.printStackTrace();
		}

	}

	private void sendZoneBlink()
	{
		getZones();
		StringBuffer sb = new StringBuffer();

		String serialNumber = "";

		serialNumber = "000000000000000000000" + cu.prompt( "enter a serialNumber", lastSerialNumber );
		lastSerialNumber = serialNumber.substring( serialNumber.length() - 21, serialNumber.length() );
		serialNumber = lastSerialNumber;

		defaultRfidThingTypeCode = cu.prompt( "enter the thingTypeCode", defaultRfidThingTypeCode );

		zoneField = cu.prompt( "enter the udf name", zoneField );

		String zoneValue = getRandomZone();
		zoneValue = cu.prompt( "enter the Zone Value", zoneValue );

		String topic = "/v1/data/ALEB/" + defaultRfidThingTypeCode;

		sequenceNumber = cu.getSequenceNumber();
		Long time = new Date().getTime();
		sb.append( " sn," + sequenceNumber + "\n" );
		sb.append( ",0,___CS___,-118.443969;34.048092;0.0;20.0;ft\n" );

		sb.append( serialNumber + "," + time + "," + zoneField + "," + "\"" + zoneValue + "\"\n" );

		System.out.println( " serialNumber: " + cu.blue() + serialNumber + cu.black() + "" );
		System.out.println( "thingTypeCode: " + cu.blue() + defaultRfidThingTypeCode + cu.black() + "" );
		System.out.println( "        field: " + cu.blue() + zoneField + cu.black() + "" );
		System.out.println( "        value: " + cu.blue() + zoneValue + cu.black() + "" );

		DBObject prevThing = cu.getThing( serialNumber, defaultRfidThingTypeCode );

		cu.publishSyncMessage( topic, sb.toString() );
		cu.sleep( 1000 );

		DBObject newThing = cu.getThing( serialNumber, defaultRfidThingTypeCode );
		cu.diffThings( newThing, prevThing );
	}

	private void sendMultipleZoneBlink()
	{
		getZones();
		StringBuffer sb = new StringBuffer();

		String serialNumber = "";

		serialNumber = "000000000000000000000" + cu.prompt( "enter a serialNumber", lastSerialNumber );
		lastSerialNumber = serialNumber.substring( serialNumber.length() - 21, serialNumber.length() );
		serialNumber = lastSerialNumber;

		multipleThingTypeCode = cu.prompt( "enter the thingTypeCode", multipleThingTypeCode );

		multiplezoneField = cu.prompt( "enter the udf name", multiplezoneField );

		String zoneValue = getMultipleRandomZone();
		zoneValue = cu.prompt( "enter the Zone Value", zoneValue );

		String topic = "/v1/data/ALEB/" + multipleThingTypeCode;

		sequenceNumber = cu.getSequenceNumber();
		Long time = new Date().getTime();
		sb.append( " sn," + sequenceNumber + "\n" );
		sb.append( ",0,___CS___,-118.443969;34.048092;0.0;20.0;ft\n" );

		sb.append( serialNumber + "," + time + "," + multiplezoneField + "," + "\"" + zoneValue + "\"\n" );

		System.out.println( " serialNumber: " + cu.blue() + serialNumber + cu.black() + "" );
		System.out.println( "thingTypeCode: " + cu.blue() + multipleThingTypeCode + cu.black() + "" );
		System.out.println( "        field: " + cu.blue() + multiplezoneField + cu.black() + "" );
		System.out.println( "        value: " + cu.blue() + zoneValue + cu.black() + "" );

		DBObject prevThing = cu.getThing( serialNumber, multipleThingTypeCode );

		cu.publishSyncMessage( topic, sb.toString() );
		cu.sleep( 1000 );

		DBObject newThing = cu.getThing( serialNumber, multipleThingTypeCode );
		cu.diffThings( newThing, prevThing );
	}

	private String getRandomShift()
	{
		Random r = new Random();

		if (shifts.size() == 0)
		{
			System.out.println("Error, the shifts list is empty!");
		}

		int index = r.nextInt( shifts.size());

		HashMap<String,Object> entry = (HashMap<String,Object>) shifts.get(index);
		if (r.nextDouble() < 0.5)
		{
			return  entry.get( "name" ).toString();
		} else {
			return entry.get( "id" ).toString();
		}

	}

	private String getMultipleRandomShift()
	{
		Random r = new Random();
		StringBuilder s = new StringBuilder( "[" );
		int n = r.nextInt( 3 ) +1;
		for (int i = 0; i < n; i++ ) {
			if (!s.toString().equals( "[" )) {
				s.append( ", " );
			}
			String strId = getRandomShift();
			String value = '"' + strId + '"';
			try
			{
				value = "" + Long.valueOf( strId);
			} catch( NumberFormatException e ) {
			}

			s.append( value );
		}
		s.append( "]" );
		return s.toString();
	}

	private void getShifts()
	{
		HashMap<String,Object> res;
		try
		{
			res = cu.httpGetMessage("shift");

			shifts = (ArrayList<HashMap<String,Object>> )res.get("results");
			//System.out.println( shifts );

			System.out.println("Valid Shifts:");
			Iterator it = shifts.iterator();
			while (it.hasNext()) {
				HashMap<String,Object> entry = (HashMap<String,Object>)it.next();
				System.out.println( "    id:" + entry.get("id") + " name:" + entry.get("name") );
			}

		}
		catch( IOException e )
		{
			e.printStackTrace();
		}
		catch( URISyntaxException e )
		{
			e.printStackTrace();
		}

	}

	private void sendShiftBlink() {
		getShifts();
		StringBuffer sb = new StringBuffer();

		String serialNumber = "";

		serialNumber = "000000000000000000000" + cu.prompt( "enter a serialNumber", lastSerialNumber );
		lastSerialNumber = serialNumber.substring( serialNumber.length() - 21, serialNumber.length() );
		serialNumber = lastSerialNumber;

		defaultRfidThingTypeCode = cu.prompt( "enter the thingTypeCode", defaultRfidThingTypeCode );

		shiftField = cu.prompt( "enter the udf name", shiftField );

		String shiftValue = getRandomShift();
		shiftValue = cu.prompt( "enter the Shift Value", shiftValue );

		String topic = "/v1/data/ALEB/" + defaultRfidThingTypeCode;

		sequenceNumber = cu.getSequenceNumber();
		Long time = new Date().getTime();
		sb.append( " sn," + sequenceNumber + "\n" );
		sb.append( ",0,___CS___,-118.443969;34.048092;0.0;20.0;ft\n" );

		sb.append( serialNumber + "," + time + "," + shiftField + "," + "\"" + shiftValue + "\"\n" );

		System.out.println( " serialNumber: " + cu.blue() + serialNumber + cu.black() + "" );
		System.out.println( "thingTypeCode: " + cu.blue() + defaultRfidThingTypeCode + cu.black() + "" );
		System.out.println( "        field: " + cu.blue() + shiftField + cu.black() + "" );
		System.out.println( "        value: " + cu.blue() + shiftValue + cu.black() + "" );

		DBObject prevThing = cu.getThing( serialNumber, defaultRfidThingTypeCode );

		cu.publishSyncMessage( topic, sb.toString() );
		cu.sleep( 1000 );

		DBObject newThing = cu.getThing( serialNumber, defaultRfidThingTypeCode );
		cu.diffThings( newThing, prevThing );
	}

	private void sendMultipleShiftBlink() {
		getShifts();
		StringBuffer sb = new StringBuffer();

		String serialNumber = "";

		serialNumber = "000000000000000000000" + cu.prompt( "enter a serialNumber", lastSerialNumber );
		lastSerialNumber = serialNumber.substring( serialNumber.length() - 21, serialNumber.length() );
		serialNumber = lastSerialNumber;

		multipleThingTypeCode = cu.prompt( "enter the thingTypeCode", multipleThingTypeCode );

		multipleshiftField = cu.prompt( "enter the udf name", multipleshiftField );

		String shiftValue = getMultipleRandomShift();
		shiftValue = cu.prompt( "enter the Shift Value", shiftValue );

		String topic = "/v1/data/ALEB/" + multipleThingTypeCode;

		sequenceNumber = cu.getSequenceNumber();
		Long time = new Date().getTime();
		sb.append( " sn," + sequenceNumber + "\n" );
		sb.append( ",0,___CS___,-118.443969;34.048092;0.0;20.0;ft\n" );

		sb.append( serialNumber + "," + time + "," + multipleshiftField + "," + "\"" + shiftValue + "\"\n" );

		System.out.println( " serialNumber: " + cu.blue() + serialNumber + cu.black() + "" );
		System.out.println( "thingTypeCode: " + cu.blue() + multipleThingTypeCode + cu.black() + "" );
		System.out.println( "        field: " + cu.blue() + multipleshiftField + cu.black() + "" );
		System.out.println( "        value: " + cu.blue() + shiftValue + cu.black() + "" );

		DBObject prevThing = cu.getThing( serialNumber, multipleThingTypeCode );

		cu.publishSyncMessage( topic, sb.toString() );
		cu.sleep( 1000 );

		DBObject newThing = cu.getThing( serialNumber, multipleThingTypeCode );
		cu.diffThings( newThing, prevThing );
	}

	private void sendJSONBlink() {
		StringBuffer sb = new StringBuffer();

		Random r = new Random();
		String serialNumber = "";

		serialNumber = "000000000000000000000" + cu.prompt( "enter a serialNumber",lastSerialNumber );
		lastSerialNumber = serialNumber.substring(serialNumber.length()-21, serialNumber.length());
		serialNumber = lastSerialNumber;

		thingTypeCode = cu.prompt( "enter the thingTypeCode",thingTypeCode );

		thingFieldJSON = cu.prompt( "enter the udf name",thingFieldJSON );

		String fruits[] = {"apples", "oranges", "bananas", "grapes", "strawberries", "watermelon", "pineapples"};
		String commaValue = "";
		for (int i = 0; i < r.nextInt( 2 )+2; i++) {
			commaValue += (commaValue.equals( "" ) ? "" : ", " ) + (r.nextInt( 10 ) + 1) + " " + fruits[ r.nextInt( fruits.length -1)];
		}

		String topic = "/v1/data/ALEB/" + thingTypeCode;

		StringBuffer sbjs = new StringBuffer(  );

		String ISOTime = "2015-08-21T12:22:22Z";

		sbjs.append( "[" );
		sbjs.append( "{" );
		sbjs.append( "\"id\":29," );
		sbjs.append( "\"name\":\"SHIFT 1\"," );
		sbjs.append( "\"active\":true," );
		sbjs.append( "\"daysofweek\":\"23456\"," );
		sbjs.append( "\"time\":\"" + ISOTime + "\"," );
		sbjs.append( "\"fruits\":\"" + commaValue + "\"" );
		sbjs.append( "}" );
		sbjs.append( "{" );
		sbjs.append( "\"id\":29," );
		sbjs.append( "\"name\":\"SHIFT 2\"," );
		sbjs.append( "\"active\":false," );
		sbjs.append( "\"daysofweek\":\"17\"," );
		sbjs.append( "\"time\":\"" + ISOTime + "\"" );
		sbjs.append( "}" );
		sbjs.append( "]" );

		StringBuilder jsonStr = new StringBuilder(  );
		for (int i = 0; i < sbjs.toString().length(); i++ )
		{
			char car = sbjs.toString().charAt( i );
			if ('"' == car)
			{
				jsonStr.append( '\\' );
				jsonStr.append( '"' );
			} else {
				jsonStr.append( car );
			}
		}

		sequenceNumber = cu.getSequenceNumber();

		Long time = new Date().getTime();
		sb.append( " sn," + sequenceNumber + "\n" );
		sb.append(",0,___CS___,-118.443969;34.048092;0.0;20.0;ft\n");

		sb.append(serialNumber + "," + time + "," + thingFieldJSON + "," + "\"" + jsonStr.toString() + "\"\n");


		System.out.println(" serialNumber: " + cu.blue() + serialNumber + cu.black() + "");
		System.out.println("thingTypeCode: " + cu.blue() + thingTypeCode + cu.black() + "");
		System.out.println("        field: " + cu.blue() + thingFieldJSON + cu.black() + "");
		System.out.println("        value: " + cu.blue() + jsonStr.toString() + cu.black() + "");

		DBObject prevThing = cu.getThing( serialNumber, thingTypeCode );

		cu.publishSyncMessage(topic, sb.toString());
		cu.sleep( 1000 );

		DBObject newThing = cu.getThing( serialNumber, thingTypeCode );
		cu.diffThings( newThing, prevThing );
	}

	public void execute() {
		setup();
		HashMap<String, String> options = new LinkedHashMap<String,String>();

		options.put("1", "create ThingTypes");
		options.put("2", "send a CSV (comma separate values) to udf");
		options.put("3", "send simple LogicalReader to Default RFID Thingtype");
		options.put("4", "send simple Zone to Default RFID Thingtype");
		options.put("5", "send simple Shift to Default RFID Thingtype");
		options.put("6", "send multiple LogicalReader to Multiple Native Object");
		options.put("7", "send multiple Zone to Multiple Native Object");
		options.put("8", "send multiple Shift to Multiple Native Object");
		//options.put("9", "send a JSON to udf");

		Integer option = 0;
		while (option != null) {
			option = cu.showMenu("Native Objects options", options );
			if (option != null) {
				if (option == 0) {
					createThingTypes();
				}

				if (option == 1) {
					sendCommasBlink();
				}

				if (option == 2) {
					sendLogicalReaderBlink();
				}

				if (option == 3) {
					sendZoneBlink();
				}

				if (option == 4) {
					sendShiftBlink();
				}

				if (option == 5) {
					sendMultipleLogicalReaderBlink();
				}

				if (option == 6) {
					sendMultipleZoneBlink();
				}

				if (option == 7) {
					sendMultipleShiftBlink();
				}

				//if (option == 5) {
				//	sendJSONBlink();
				//}

				System.out.println(cu.black() +  "\npress [enter] to continue");
				Scanner in = new Scanner(System.in);
				in.nextLine();
			}
		}

	}

}
