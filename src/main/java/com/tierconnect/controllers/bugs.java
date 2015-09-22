package com.tierconnect.controllers;

import com.mongodb.DBCollection;
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

	String defaultRfidThingTypeCode = "default_rfid_thingtype";
	String thingTypeCode = "default_gps_thingtype";

	ArrayList<HashMap<String,Object>> logicalReaders;

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


	public void execute() {
		setup();
		HashMap<String, String> options = new LinkedHashMap<String,String>();

		options.put("1", "RIOT-5841 Inconsistencies in Mysql, duplicated things");
		options.put("2", "RIOT-6241 Door Event Filters");
		//options.put("9", "send a JSON to udf");

		Integer option = 0;
		while (option != null) {
			option = cu.showMenu("known bugs", options );
			if (option != null) {
				if (option == 0 )
				{
					instantiateMany();
				}

				System.out.println(cu.black() +  "\npress [enter] to continue");
				Scanner in = new Scanner(System.in);
				in.nextLine();
			}
		}

	}

}
