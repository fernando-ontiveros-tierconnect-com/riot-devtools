package com.tierconnect.controllers;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.tierconnect.dev.controllerInterface;
import com.tierconnect.utils.CommonUtils;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Random;
import java.util.Scanner;

/**
 * Created by fernando on 7/17/15.
 */
public class bugs implements controllerInterface
{
	CommonUtils cu;
	String lastSerialNumber = "000000000000000010000";

	Long sequenceNumber = 0L;
	Long serialNumber = 200L;

	DBCollection thingsCollection;

	Date startDate;

	String defaultRfidThingTypeCode = "default_rfid_thingtype";
	String thingTypeCode = "default_gps_thingtype";

	public void setCu( CommonUtils cu )
	{
		this.cu = cu;
	}

	public String getDescription()
	{

		return "test cases for known bugs";
	}

	public void setup()
	{
		thingsCollection = cu.db.getCollection( "things" );

		cu.defaultMqttConnection();
	}

	public void instantiateMany() {
		Long baseSerial, serial;
		String serialNumber1, serialNumber2;
		Random r = new Random(  );
		String status;
		StringBuffer sb = new StringBuffer();

		System.out.println("Exploit the issue RIOT-5841 Sharaf > Inconsistencies in Mongo, duplicated things");
		//get thingtype code
		defaultRfidThingTypeCode = cu.prompt( "enter the thingTypeCode", defaultRfidThingTypeCode );

		lastSerialNumber = cu.getLastSerialForThingType(defaultRfidThingTypeCode);

		lastSerialNumber = cu.formatSerialNumber( cu.prompt( "enter Starting serialNumber", lastSerialNumber ));

		baseSerial = Long.valueOf( lastSerialNumber );

		sequenceNumber = cu.getSequenceNumber();
		Long time = new Date().getTime();

		HashMap<String,Object> res;

		String thingTypeId = "1";
		String groupId = "3";

		System.out.println( cu.blue() + "sending two new things to the endpoint, but the second serial is sent twice" + cu.black());

		StringBuffer body = new StringBuffer();

		serial = baseSerial ;
		serialNumber1 = cu.formatSerialNumber( serial + "" );
		serialNumber2 = cu.formatSerialNumber( (serial+1) + "" );

		//sending twice and then sending another one
		body.append( serialNumber1 + "\n" );
		body.append( serialNumber2 + "\n" );
		body.append( serialNumber2 + "\n" );
		System.out.println(body.toString());

		try {
			cu.httpPostMessage( "thingType/many/" + thingTypeId + "?groupId=" + groupId, body.toString() );

		} catch (Exception e) {
			System.out.println(e.getCause());
		}

		System.out.println( cu.green() + "two things were created in Mongo" + cu.black() + ", but now check if both things were created in Mysql" + cu.black());
		DBObject prevThing;
		prevThing = cu.getThing( serialNumber1, defaultRfidThingTypeCode );
		cu.displayThing( prevThing );
		Long t1 = (Long)prevThing.get("_id");

		prevThing = cu.getThing( serialNumber2, defaultRfidThingTypeCode );
		cu.displayThing( prevThing );
		Long t2 = (Long)prevThing.get("_id");

		try {
			System.out.println( cu.httpGetMessage( "thing/" + t1 ));
			System.out.println( cu.httpGetMessage( "thing/" + t2 ));

		} catch (Exception e) {
			System.out.println(e.getCause());
		}

		//now creating another two things
		try {
			serialNumber1 = cu.formatSerialNumber( (serial+2) + "" );
			serialNumber2 = cu.formatSerialNumber( (serial+1) + "" );

			//sending twice and then sending another one
			body = new StringBuffer(  );
			body.append( serialNumber1 + "\n" );
			body.append( serialNumber2 + "\n" );
			System.out.println(body.toString());
			cu.httpPostMessage( "thingType/many/" + thingTypeId + "?groupId=" + groupId, body.toString() );

			prevThing = cu.getThing( serialNumber1, defaultRfidThingTypeCode );
			cu.displayThing( prevThing );
			t1 = (Long)prevThing.get("_id");

			prevThing = cu.getThing( serialNumber2, defaultRfidThingTypeCode );
			cu.displayThing( prevThing );
			t2 = (Long)prevThing.get("_id");
			System.out.println( cu.httpGetMessage( "thing/" + t1 ));
			System.out.println( cu.httpGetMessage( "thing/" + t2 ));

		} catch (Exception e) {
			System.out.println(e.getCause());
		}

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


	private void sendCommasBlink()
	{
		StringBuffer sb = new StringBuffer();

		Random r = new Random();
		String serialNumber = "";

		serialNumber = "000000000000000000000" + cu.prompt( "enter a serialNumber", lastSerialNumber );
		lastSerialNumber = serialNumber.substring( serialNumber.length() - 21, serialNumber.length() );
		serialNumber = lastSerialNumber;

		defaultRfidThingTypeCode = cu.prompt( "enter the thingTypeCode", defaultRfidThingTypeCode );

		//statusThingField = cu.prompt( "enter the udf name", statusThingField );

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

		sb.append( serialNumber + "," + time + "," + "status" + "," + "\"" + commaValue + "\"\n" );

		System.out.println( " serialNumber: " + cu.blue() + serialNumber + cu.black() + "" );
		System.out.println( "thingTypeCode: " + cu.blue() + defaultRfidThingTypeCode + cu.black() + "" );
		//System.out.println( "        field: " + cu.blue() + statusThingField + cu.black() + "" );
		System.out.println( "        value: " + cu.blue() + commaValue + cu.black() + "" );

		DBObject prevThing = cu.getThing( serialNumber, defaultRfidThingTypeCode );

		cu.publishSyncMessage( topic, sb.toString() );
		cu.sleep( 1000 );

		DBObject newThing = cu.getThing( serialNumber, defaultRfidThingTypeCode );
		cu.diffThings( newThing, prevThing );
	}

	public void doorEvents()
	{
		cu.getLogicalReaders();
		String lr = cu.getRandomLRCode();

		StringBuffer sb = new StringBuffer();
		Scanner in;
		in = new Scanner(System.in);

		Random r = new Random();
		String serialNumber = "";

		final String defaultRfidThingTypeCode = "default_rfid_thingtype";
		String topic = "/v1/data/ALEB/" + defaultRfidThingTypeCode;
		Long time = new Date().getTime();

		lastSerialNumber = cu.getLastSerialForThingType(defaultRfidThingTypeCode);

		lastSerialNumber = cu.formatSerialNumber( cu.prompt( "enter serialNumber", lastSerialNumber ));
		serialNumber = lastSerialNumber;

		sequenceNumber = cu.getSequenceNumber();

		String inOut= "out";

		if (r.nextDouble() < 0.50 ) {
			inOut = "in";
		}

		sb.append( " sn," + sequenceNumber + "\n" );
		sb.append( ",0,___CS___,-118.443969;34.048092;0.0;20.0;ft\n" );

		sb.append( serialNumber + "," + time + ",doorEvent," + lr + ":" + inOut + "\n" );

		System.out.println( "  serialNumber: " + cu.blue() + serialNumber + cu.black() + "" );
		System.out.println( " thingTypeCode: " + cu.blue() + defaultRfidThingTypeCode + cu.black() + "" );
		System.out.println( "logical reader: " + cu.blue() + lr + cu.black() + "" );
		System.out.println( "         value: " + cu.blue() + inOut + cu.black() + "" );

		DBObject prevThing = cu.getThing( serialNumber, defaultRfidThingTypeCode );

		cu.publishSyncMessage( topic, sb.toString() );
		cu.sleep( 1000 );

		cu.diffThings(cu.getThing(serialNumber, defaultRfidThingTypeCode), prevThing);

	}

	public void resendMessages()
	{
		StringBuffer sb = new StringBuffer();
		Scanner in;
		in = new Scanner(System.in);

		Random r = new Random();
		String serialNumber = "";

		final String defaultRfidThingTypeCode = "default_rfid_thingtype";
		String topic = "/v1/data/ALEB/" + defaultRfidThingTypeCode;
		Long time = new Date().getTime();

		lastSerialNumber = cu.getLastSerialForThingType(defaultRfidThingTypeCode);

		lastSerialNumber = cu.formatSerialNumber( cu.prompt( "enter serialNumber", lastSerialNumber ));
		serialNumber = lastSerialNumber;

		sequenceNumber = cu.getSequenceNumber();

		String inOut= "status " + r.nextInt( 900 ) + 100;

		sb.append( "sn," + sequenceNumber + "\n" );
		sb.append( ",0,___CS___,-118.443969;34.048092;0.0;20.0;ft\n" );
		sb.append( serialNumber + "," + time + ",status," + inOut + "\n" );

		System.out.println( "\nsending this message to default MQTT broker: \n" + cu.blue() + sb.toString() + cu.black() );

		DBObject prevThing = cu.getThing( serialNumber, defaultRfidThingTypeCode );

		cu.publishSyncMessage( topic, sb.toString() );
		cu.sleep( 1000 );
		System.out.println( "\nnow check with mosquitto_sub that message was sent" );
		System.out.println( "also check in CoreBridge logs that the blink was processed."  );

		System.out.println(cu.black() +  "\npress [enter] to resend the message");

		in.nextLine();

		cu.publishSyncMessage( topic, sb.toString() );
		cu.sleep( 1000 );

		System.out.println( "\n now check with mosquitto_sub that message was sent"  );
		System.out.println( "also check in CoreBridge logs that the blink was NOT processed \n"  );

		cu.sleep( 2000 );
		cu.diffThings(cu.getThing(serialNumber, defaultRfidThingTypeCode), prevThing);

	}

	private String displayTime( Date time)
	{
		DateFormat writeFormat = new SimpleDateFormat( "HH:mm:ss");
		String formattedDate = writeFormat.format( time );
		return formattedDate;
	}

	public void showResults(String serialNumber)
	{
//		cu.diffThings(cu.getThing(serialNumber, defaultRfidThingTypeCode), prevThing);
		DBCollection ts = cu.db.getCollection( "thingSnapshots" );

		DBCursor cursor = ts.find( new BasicDBObject( "value.serialNumber", serialNumber )).sort( new BasicDBObject("time", -1));

		while (cursor.hasNext())
		{
			BasicDBObject snapshot = (BasicDBObject)cursor.next();
			BasicDBObject value  = (BasicDBObject)snapshot.get("value");
			Date time   = (Date)snapshot.get("time");
			BasicDBObject zone   = (BasicDBObject)value.get("zone");
			BasicDBObject lr     = (BasicDBObject)value.get("logicalReader");
			BasicDBObject status = (BasicDBObject)value.get("status");

			System.out.print ("||");

			//zone
			System.out.print (cu.ltrim( displayTime( time ), 8 ) + "|");
			if (zone != null)
			{
				BasicDBObject zoneObj = (BasicDBObject)zone.get( "value" );
				System.out.print( cu.ltrim( zoneObj.get( "name" ).toString(), 15 ) );
			} else
			{
				System.out.print( cu.ltrim( " ", 15 ) );
			}
			//System.out.print( "|" );
			if (zone != null)
			{
				Long dwellTime = Long.parseLong( zone.get( "dwellTime" ).toString() ) + 4*60*60*1000;
				System.out.print( cu.ltrim( displayTime( new Date(dwellTime) ), 9 ));
			} else {
				System.out.print( cu.ltrim( " ", 9 ));
			}
			System.out.print ("|");

			//lr
			if (lr != null)
			{
				BasicDBObject lrObj = (BasicDBObject)lr.get( "value" );
				System.out.print( cu.ltrim( lrObj.get( "code" ).toString(), 10 ) );
			} else {
				System.out.print( cu.ltrim( " ", 10 ));
			}
			//System.out.print( "|" );
			if (lr != null)
			{
				Long dwellTime = Long.parseLong( lr.get( "dwellTime" ).toString() ) + 4*60*60*1000;
				System.out.print( cu.ltrim( displayTime( new Date(dwellTime) ), 9 ));
			} else {
				System.out.print( cu.ltrim( " ", 9 ));
			}
			System.out.print ("|");

			//status
			if (status != null)
			{
				System.out.print( cu.ltrim( status.get( "value" ).toString(), 10 ) );
			} else {
				System.out.print( cu.ltrim( " ", 10 ) );
			}
			//System.out.print( "|" );
			if (status != null)
			{
				Long dwellTime = Long.parseLong( status.get( "dwellTime" ).toString() ) + 4*60*60*1000;
				System.out.print( cu.ltrim( displayTime( new Date(dwellTime) ), 9 ));
			} else {
				System.out.print( cu.ltrim( " ", 9 ));
			}
			System.out.println( "|" );

		}
		System.out.println();
	}

	public int showHttpPoolResults()
	{
		DBCollection ts = cu.db.getCollection( "httpPoolTest" );

		DBCursor cursor = ts.find( new BasicDBObject() ).sort( new BasicDBObject("serialNumber", -1));

		int sum = 0, td = 0;

		while (cursor.hasNext())
		{
			BasicDBObject record = (BasicDBObject) cursor.next();

			System.out.print( "|" );
			System.out.print( cu.rtrim( record.get( "serialNumber" ), 18 ) );

			System.out.print( "|" );
			System.out.print( cu.ltrim( record.get( "count" ), 6 ) );

			sum += Integer.parseInt( record.get( "count" ).toString());

			td ++;
			if (td % 4 == 0)
			{
				System.out.println( "|" );
			}
		}
		System.out.println( );
		return sum;
	}

	public void showResultsExtended(String serialNumber)
	{
		DBCollection ts = cu.db.getCollection( "thingSnapshots" );

		DBCursor cursor = ts.find( new BasicDBObject( "value.serialNumber", serialNumber )).sort( new BasicDBObject("time", -1));

		while (cursor.hasNext())
		{
			BasicDBObject snapshot = (BasicDBObject)cursor.next();
			BasicDBObject value  = (BasicDBObject)snapshot.get("value");
			Date time   = (Date)snapshot.get("time");
			BasicDBObject zone   = (BasicDBObject)value.get("zone");
			BasicDBObject registered = (BasicDBObject)value.get("registered");
			BasicDBObject status = (BasicDBObject)value.get("status");
			BasicDBObject enode = (BasicDBObject)value.get("eNode");

			System.out.print ("||");

			//zone
			System.out.print (cu.ltrim( displayTime( time ), 8 ) + "|");
			if (zone != null)
			{
				BasicDBObject zoneObj = (BasicDBObject)zone.get( "value" );
				System.out.print( cu.ltrim( zoneObj.get( "name" ).toString(), 15 ) );
			} else
			{
				System.out.print( cu.ltrim( " ", 15 ) );
			}

			if (zone != null)
			{
				Long dwellTime = Long.parseLong( zone.get( "dwellTime" ).toString() ) + 4*60*60*1000;
				System.out.print( cu.ltrim( displayTime( new Date(dwellTime) ), 9 ));
			} else {
				System.out.print( cu.ltrim( " ", 9 ));
			}
			System.out.print ("|");

			//status
			if (status != null)
			{
				System.out.print( cu.ltrim( status.get( "value" ).toString(), 10 ) );
			} else {
				System.out.print( cu.ltrim( " ", 10 ) );
			}
			//System.out.print( "|" );
			if (status != null)
			{
				Long dwellTime = Long.parseLong( status.get( "dwellTime" ).toString() ) + 4*60*60*1000;
				System.out.print( cu.ltrim( displayTime( new Date(dwellTime) ), 9 ));
			} else {
				System.out.print( cu.ltrim( " ", 9 ));
			}
			System.out.print ("|");

			//registered
			if (registered != null)
			{
				System.out.print( cu.ltrim( registered.get( "value" ), 10 ) );
				Long dwellTime = Long.parseLong( registered.get( "dwellTime" ).toString() ) + 4*60*60*1000;
				System.out.print( cu.ltrim( displayTime( new Date(dwellTime) ), 9 ));
			} else {
				System.out.print( cu.ltrim( " ", 10 ));
				System.out.print( cu.ltrim( " ", 9 ));
			}
			System.out.print ("|");

			//enode
			if (enode != null)
			{
				System.out.print( cu.ltrim( enode.get( "value" ), 10 ) );
				Long dwellTime = Long.parseLong( enode.get( "dwellTime" ).toString() ) + 4*60*60*1000;
				System.out.print( cu.ltrim( displayTime( new Date(dwellTime) ), 9 ));
			} else {
				System.out.print( cu.ltrim( " ", 10 ));
				System.out.print( cu.ltrim( " ", 9 ));
			}
			System.out.print ("|");

			System.out.println( "|" );

		}
		System.out.println();
	}


	public String getZone1( String serialNumber, Long time)
	{
		String res = "";
		res += serialNumber + "," + time + ",location,-118.44395517462448;34.04811656588989;0.0" + "\n";
		res += serialNumber + "," + time + ",locationXYZ,7.0;7.0;0.0" + "\n";
		return res;
	}

	public String getZone2( String serialNumber, Long time)
	{
		String res = "";
		res += serialNumber + "," + time + ",location,-118.44381404397095;34.04821157541592;0.0" + "\n";
		res += serialNumber + "," + time + ",locationXYZ,59.0;25.0;0.0" + "\n";
		return res;
	}

	public String getZone3( String serialNumber, Long time)
	{
		String res = "";
		res += serialNumber + "," + time + ",location,-118.44390383463396;34.048239858851375;0.0" + "\n";
		res += serialNumber + "," + time + ",locationXYZ,37.0;44.0;0.0" + "\n";
		return res;
	}

	public String getZone4( String serialNumber, Long time)
	{
		String res = "";
		res += serialNumber + "," + time + ",location,-118.44392666764658;34.048139731794365;0.0" + "\n";
		res += serialNumber + "," + time + ",locationXYZ,18.0;12.0;0.0" + "\n";
		return res;
	}

	public void sendBlinkChangingZone (String topic, Long time, String serialNumber)
	{
		sendBlinks( topic, time, serialNumber, true, false );
	}

	public void sendBlinkChangingLR (String topic, Long time, String serialNumber)
	{
		sendBlinks( topic, time, serialNumber, false, true );
	}

	public void sendBlinkChangingZoneAndLR(String topic, Long time, String serialNumber)
	{
		sendBlinks( topic, time, serialNumber, true, true );
	}

	public void sendBlinks(String topic, Long time, String serialNumber, boolean zone, boolean lr )
	{
		StringBuffer sb = new StringBuffer();
		Random r = new Random();

		String inOut= "out";

		if (r.nextDouble() < 0.50 ) {
			inOut = "in";
		}

		sequenceNumber = cu.getSequenceNumber();
		sb.append( " sn," + sequenceNumber + "\n" );
		sb.append( ",0,___CS___,-118.443969;34.048092;0.0;20.0;ft\n" );

		System.out.println(	cu.blue() + "sn," + sequenceNumber + cu.black() );

		if (zone)
		{
			int zr = r.nextInt( 100 )%4;

			String zoneValue = "";
			switch( zr ) {
				case 0 : zoneValue = getZone1(serialNumber, time);
					break;
				case 1 : zoneValue = getZone2( serialNumber, time );
					break;
				case 2 : zoneValue = getZone3( serialNumber, time );
					break;
				case 3 : zoneValue = getZone4( serialNumber, time );
					break;
			}
			sb.append( zoneValue );

			System.out.print(  zoneValue );

		}

		if (lr)
		{
			String lrValue = cu.getRandomLRCode();
			sequenceNumber = cu.getSequenceNumber();
			sb.append( serialNumber + "," + time + ",logicalReader," + lrValue + "\n" );

			System.out.println(  serialNumber + "," + time + ",lr," + lrValue  );

		}
		cu.publishSyncMessage( topic, sb.toString() );
		System.out.println( );

	}


	public void sendBlinkChangingStatus(String topic, Long time, String serialNumber, Integer retries )
	{
		StringBuffer sb = new StringBuffer();
		Random r = new Random();

		sequenceNumber = cu.getSequenceNumber();
		sb.append( " sn," + sequenceNumber + "\n" );
		sb.append( ",0,___CS___,-118.443969;34.048092;0.0;20.0;ft\n" );

		System.out.println(	cu.blue() + "sn," + sequenceNumber + cu.black() );

		int zr = r.nextInt( 100 )%4;

		String zoneValue = "";
		switch( zr ) {
			case 0 : zoneValue = getZone1( serialNumber, time );
				break;
			case 1 : zoneValue = getZone2( serialNumber, time );
				break;
			case 2 : zoneValue = getZone3( serialNumber, time );
				break;
			case 3 : zoneValue = getZone4( serialNumber, time );
				break;
		}
		sb.append( zoneValue );

		System.out.print(  zoneValue );

		//status
		String statusValue = "changeStatus";
		if (retries >= 2) {
			statusValue = "SapSync";
		}
		sb.append( serialNumber + "," + time + ",status," + statusValue + "\n" );

		System.out.println(  serialNumber + "," + time + ",status," + statusValue  );

		//retries - registered
		String retriesValue = "" + retries;
		sb.append( serialNumber + "," + time + ",registered," + retriesValue + "\n" );

		System.out.println(  serialNumber + "," + time + ",registered," + retriesValue  );

		cu.publishSyncMessage( topic, sb.toString() );
		System.out.println( );

	}

	public void sendBlinkChangingEnode(String topic, Long time, String serialNumber, String message )
	{
		StringBuffer sb = new StringBuffer();
		Random r = new Random();

		sequenceNumber = cu.getSequenceNumber();
		sb.append( " sn," + sequenceNumber + "\n" );
		sb.append( ",0,___CS___,-118.443969;34.048092;0.0;20.0;ft\n" );

		System.out.println(	cu.blue() + "sn," + sequenceNumber + cu.black() );

		//enode
		sb.append( serialNumber + "," + time + ",eNode," + message + "\n" );

		System.out.println(  serialNumber + "," + time + ",eNode," + message  );

		cu.publishSyncMessage( topic, sb.toString() );
		System.out.println( );

	}

	public void sendBlinkInvalidNativeObjects(String topic, Long time, String serialNumber )
	{
		StringBuffer sb = new StringBuffer();
		Random r = new Random();

		sequenceNumber = cu.getSequenceNumber();
		sb.append( " sn," + sequenceNumber + "\n" );
		sb.append( ",0,___CS___,-118.443969;34.048092;0.0;20.0;ft\n" );

		System.out.println(	cu.blue() + "sn," + sequenceNumber + cu.black() );

		String zoneValue  = "zone-"  + (r.nextInt( 900 )+100);
		String shiftValue = "shift-" + (r.nextInt( 900 )+100);
		String lrValue    = "lr-"    + (r.nextInt( 900 )+100);
		String groupValue = "group-" + (r.nextInt( 900 )+100);
		String status = "(" + zoneValue + "," + shiftValue + "," + lrValue + "," + groupValue + ")";

		sequenceNumber = cu.getSequenceNumber();

		sb.append( serialNumber + "," + time + ",zone,"          + zoneValue + "\n" );
		sb.append( serialNumber + "," + time + ",shift,"         + shiftValue + "\n" );
		sb.append( serialNumber + "," + time + ",logicalReader," + lrValue + "\n" );
		sb.append( serialNumber + "," + time + ",group,"         + groupValue + "\n" );
		sb.append( serialNumber + "," + time + ",status,"        + "\"" + status + "\"" + "\n" );

		cu.publishSyncMessage( topic, sb.toString() );
	}

	public void sendSimpleBlinks(String topic, Long time, String serialNumber )
	{
		StringBuffer sb = new StringBuffer();
		Random r = new Random();

		sequenceNumber = cu.getSequenceNumber();
		sb.append( " sn," + sequenceNumber + "\n" );
		sb.append( ",0,___CS___,-118.443969;34.048092;0.0;20.0;ft\n" );

		//System.out.println(	cu.blue() + "sn," + sequenceNumber + cu.black() + " " + serialNumber);

			int zr = r.nextInt( 100 )%4;

			String zoneValue = "";
			switch( zr ) {
				case 0 : zoneValue = getZone1(serialNumber, time);
					break;
				case 1 : zoneValue = getZone2( serialNumber, time );
					break;
				case 2 : zoneValue = getZone3( serialNumber, time );
					break;
				case 3 : zoneValue = getZone4( serialNumber, time );
					break;
			}
			sb.append( zoneValue );

		cu.publishSyncMessage( topic, sb.toString() );
	}

	public void pressAnyKey()
	{
		System.out.println(cu.black() +  "\npress [enter] to continue");
		Scanner in = new Scanner(System.in);
		in.nextLine();

	}

	public long getNextSecond10()
	{
		Date now = new Date();
		while( now.getTime() % 10000 != 0 )
		{
			now = new Date();
		}
		;
		return now.getTime();
	}

	public long getNextSecond5()
	{
		Date now = new Date();
		while( now.getTime() % 5000 != 0 )
		{
			now = new Date();
		}
		;
		return now.getTime();
	}

	public void dwellTimeIssue()
	{
		//send four blinks each 10 seconds, modifying zone and logicalreader
		//wait for a change in UI
		//send one blink changing logical reader
		//send second blink changing zone
		//show results

		cu.getLogicalReaders();

		String lr = cu.getRandomLRCode();

		Scanner in;
		in = new Scanner(System.in);

		String serialNumber = "";

		final String defaultRfidThingTypeCode = "default_rfid_thingtype";

		lastSerialNumber = cu.getLastSerialForThingType(defaultRfidThingTypeCode);

		lastSerialNumber = cu.formatSerialNumber( cu.prompt( "enter serialNumber", lastSerialNumber ));
		serialNumber = lastSerialNumber;

		String topic = "/v1/data/ALEB/" + defaultRfidThingTypeCode;
		Long time;

		System.out.println( );
		System.out.println( cu.blue() + "1. send four blinks each 10 seconds, modifying zone and logical reader" + cu.black() );

		time = getNextSecond10();
		sendBlinkChangingZone( topic, time, serialNumber );
		cu.sleep( 3000 );
		showResults( serialNumber );

		time = getNextSecond10();
		sendBlinkChangingLR( topic, time, serialNumber );
		cu.sleep( 3000 );
		showResults( serialNumber);

		time = getNextSecond10();
		sendBlinkChangingZoneAndLR(topic, time, serialNumber);
		cu.sleep( 3000 );
		showResults( serialNumber);

		time = getNextSecond10();
		sendBlinkChangingZone( topic, time, serialNumber );
		cu.sleep( 3000 );
		showResults( serialNumber);

		System.out.println( cu.blue() + "2. wait until user change status udf in UI" + cu.black() );
		pressAnyKey();
		showResults( serialNumber);

		System.out.println( cu.blue() + "3. change zone" + cu.black() );
		time = getNextSecond10();
		sendBlinkChangingZone(topic, time, serialNumber);
		cu.sleep( 3000 );
		showResults( serialNumber);

		System.out.println( cu.blue() + "4. change logicalreader" + cu.black() );
		time = getNextSecond10();
		sendBlinkChangingLR(topic, time, serialNumber);
		cu.sleep( 3000 );
		showResults( serialNumber);

	}

	public void ignoreInvalidNative()
	{
		//send a full message with invalid native objects and a valid status value
		//send second blink changing zone
		//show results

		Scanner in;
		in = new Scanner(System.in);

		String serialNumber = "";

		final String defaultRfidThingTypeCode = "default_rfid_thingtype";

		lastSerialNumber = cu.getLastSerialForThingType(defaultRfidThingTypeCode);

		lastSerialNumber = cu.formatSerialNumber( cu.prompt( "enter serialNumber", lastSerialNumber ));
		serialNumber = lastSerialNumber;

		String topic = "/v1/data/ALEB/" + defaultRfidThingTypeCode;
		Long time;

		System.out.println( );
		System.out.println( cu.blue() + "1. send a message with invalid native object and a new value for status fieldr" + cu.black() );

		DBObject prevThing = cu.getThing(serialNumber, defaultRfidThingTypeCode);

		sendBlinkInvalidNativeObjects( topic, new Date().getTime(), serialNumber );
		cu.sleep( 1000 );
		cu.diffThings(cu.getThing(serialNumber, defaultRfidThingTypeCode), prevThing);
	}

	public void duplicateTickles()
	{
		//send blinks blinks each 10 seconds, modifying zone and logicalreader
		//wait for a change in UI
		//send one blink changing logical reader
		//send second blink changing zone
		//show results


		String serialNumber;

		final String defaultRfidThingTypeCode = "default_rfid_thingtype";

		lastSerialNumber = cu.getLastSerialForThingType(defaultRfidThingTypeCode);

		lastSerialNumber = cu.formatSerialNumber( cu.prompt( "enter serialNumber", lastSerialNumber ));
		serialNumber = lastSerialNumber;

		String topic = "/v1/data/ALEB/" + defaultRfidThingTypeCode;
		Long time, firstTime, firstFiveTime, nextFiveTime;

		System.out.println( );
		System.out.println( cu.blue() + "1. send five blinks each 10 seconds, with same timestamp modifying status, registered and eNode " + cu.black() );

		time = getNextSecond10();
		firstTime = time;

		sendBlinkChangingStatus( topic, firstTime, serialNumber, 1 );
		cu.sleep( 2000 );
		showResultsExtended( serialNumber );

		firstFiveTime = getNextSecond5();

		sendBlinkChangingEnode( topic, firstFiveTime, serialNumber, "error1" );
		cu.sleep( 2000 );
		showResultsExtended( serialNumber );


		getNextSecond10();
		sendBlinkChangingStatus( topic, firstTime, serialNumber, 2 );
		cu.sleep( 3000 );
		showResultsExtended( serialNumber );

		nextFiveTime = getNextSecond5();
		sendBlinkChangingEnode( topic, nextFiveTime, serialNumber, "error2" );
		cu.sleep( 2000 );
		showResultsExtended( serialNumber );

		getNextSecond10();
		sendBlinkChangingStatus( topic, firstTime, serialNumber, 3);
		cu.sleep( 3000 );
		showResultsExtended( serialNumber );

		nextFiveTime = getNextSecond5();
		sendBlinkChangingEnode( topic, nextFiveTime, serialNumber, "error3" );
		cu.sleep( 2000 );
		showResultsExtended( serialNumber );

		getNextSecond10();
		sendBlinkChangingStatus( topic, firstTime, serialNumber, 4 );
		cu.sleep( 3000 );
		showResultsExtended( serialNumber );

		nextFiveTime = getNextSecond5();
		sendBlinkChangingEnode( topic, nextFiveTime, serialNumber, "error4" );
		cu.sleep( 2000 );
		showResultsExtended( serialNumber );


		getNextSecond10();
		sendBlinkChangingStatus( topic, firstTime, serialNumber, 5);
		cu.sleep( 3000 );
		showResultsExtended( serialNumber );

		nextFiveTime = getNextSecond5();
		sendBlinkChangingEnode( topic, nextFiveTime, serialNumber, "error5" );
		cu.sleep( 2000 );
		showResultsExtended( serialNumber );

		System.out.println( cu.blue() + "complete" + cu.black() );
		pressAnyKey();
		showResults( serialNumber);


	}

	public void httpThreadPool()
	{
		//send blinks blinks each without delays to have the httpThread overloaded
		cu.db.getCollection( "httpPoolTest" ).drop();

		String serialNumber;
		Long   baseSerialNumber;
		//cu.getLogicalReaders();

		final String defaultRfidThingTypeCode = "default_rfid_thingtype";

		lastSerialNumber = cu.getLastSerialForThingType(defaultRfidThingTypeCode);

		lastSerialNumber = cu.formatSerialNumber( cu.prompt( "enter serialNumber", lastSerialNumber ));
		baseSerialNumber = Long.parseLong( lastSerialNumber);
		serialNumber = lastSerialNumber;

		Long thingsQuantity = 20L;
		thingsQuantity = Long.parseLong( cu.prompt( "enter number of things to create", "" + thingsQuantity ));

		Long blinksQuantity = 100L;
		blinksQuantity = Long.parseLong( cu.prompt( "enter number of blinks for each thing", "" + blinksQuantity ));


		System.out.println( );
		System.out.println( cu.blue() + "1. create things and update them.  Be sure that an esper rule is active for default_rfid_thingtype " + cu.black() );

		String topic = "/v1/data/ALEB/" + defaultRfidThingTypeCode;

		startDate = new Date();
		//iterate over the  things quantity, and the over the blinks
		for (int iBlinksQuantity = 0; iBlinksQuantity < blinksQuantity.intValue(); iBlinksQuantity++ ) {
			for (int iThingsQuantity = 0; iThingsQuantity < thingsQuantity.intValue(); iThingsQuantity++ ) {
				serialNumber = cu.formatSerialNumber( "" + (baseSerialNumber + iThingsQuantity) );
				sendSimpleBlinks( topic, new Date().getTime(), serialNumber );
			}
			if (iBlinksQuantity % 10 == 0)
			{
				System.out.println( "sent " + cu.blue() + (thingsQuantity * iBlinksQuantity) + cu.black() + " blinks to core bridge" );
			}
		}
		System.out.println( "sent " + cu.blue() + (thingsQuantity*blinksQuantity) + cu.black() + " blinks to core bridge" );

		Long total = 0L;
		while (total < thingsQuantity*blinksQuantity)
		{
			long diff = new Date().getTime() - startDate.getTime();
			System.out.println ( String.format( "results after %4.1f secs", diff / 1000.0  ));
			total = new Long( showHttpPoolResults() );
			System.out.println("");
			cu.sleep( 1900 );
		}

		System.out.println( cu.blue() + "complete" + cu.black() );
		pressAnyKey();
	}

	public void execute() {
		setup();
		HashMap<String, String> options = new LinkedHashMap<String,String>();

		options.put("1", "RIOT-5841 Inconsistencies in Mysql, duplicated things");
		options.put("2", "RIOT-6241 Door Event Filters");
		options.put("3", "RIOT-6337 Queued Mqtt messages are processed multiple times");
		options.put("4", "RIOT-6642 dwellTime value is not being calculated correctly for each blink");
		options.put("5", "RIOT-6779 ignore zone field when its value does not match with any existing zone");
		options.put("6", "RIOT-7385 Thing Tickle is generating duplicated values in Core Bridge");
		options.put("7", "RIOT-6809 improve RestEndpointSubscriber to use a multi threaded HTTP service");
		//options.put("9", "send a JSON to udf");

		Integer option = 0;
		while (option != null) {
			option = cu.showMenu("known bugs", options );
			if (option != null) {
				if (option == 0 )
				{
					instantiateMany();
				}

				if (option == 1 )
				{
					doorEvents();
				}

				if (option == 2 )
				{
					resendMessages();
				}

				if (option == 3 )
				{
					dwellTimeIssue();
				}

				if (option == 4 )
				{
					ignoreInvalidNative();
				}

				if (option == 5 )
				{
					duplicateTickles();
				}

				if (option == 6 )
				{
					httpThreadPool();
				}

				System.out.println(cu.black() +  "\npress [enter] to continue");
				Scanner in = new Scanner(System.in);
				in.nextLine();
			}
		}

	}

}
