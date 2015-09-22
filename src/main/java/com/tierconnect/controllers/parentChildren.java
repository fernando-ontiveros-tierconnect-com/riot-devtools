package com.tierconnect.controllers;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MapReduceCommand;
import com.mongodb.MapReduceOutput;
import com.tierconnect.dev.controllerInterface;
import com.tierconnect.utils.CommonUtils;
import com.tierconnect.utils.TimerUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.HashMap;
import java.util.Random;
import java.util.Scanner;

/**
 * Created by fernando on 7/17/15.
 */
public class parentChildren implements controllerInterface
{
	CommonUtils cu;
	String lastSerialNumber = "000000000000000010000";
	String lastQuantity = "10000";
	String lastThingsPerMessage = "100";
	Integer lastPosx = 0;
	Integer lastPosy = 0;

	Long sequenceNumber = 0L;
	Long serialNumber = 200L;
	Long errores = 0L;
	Long created = 0L;

	Long thingsToChange = 40000L;

	DBCollection thingsCollection;
	DBCollection outputCollection;
	BasicDBObject docs[];		String tag;

	String thingTypeCode = "default_rfid_thingtype";
	String thingField    = "status";
	String thingFieldJSON = "shifts";

	public void setCu(CommonUtils cu) {
		this.cu = cu;
	}


	public String getDescription() {

		return "Parent-Children functions";
	}

	public void setup()
	{
		thingsCollection        = cu.db.getCollection("things");
		outputCollection        = cu.db.getCollection("mr_reusableTag");

		cu.defaultMqttConnection();

	}


	private String read( String fname ) throws IOException
	{
		InputStream is = parentChildren.class.getResourceAsStream( fname );
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

	public void executeMR()
	{
		try {
			TimerUtils tu = new TimerUtils();
			tu.mark();
			String childrenMap = read( "/childrenMap.txt" );
			String childrenReduce = read( "/childrenReduce.txt" );

			DBObject query = new BasicDBObject();

			MapReduceCommand cmd = new MapReduceCommand(
					thingsCollection,
					childrenMap,
					childrenReduce,
					"mr_forklift",
					MapReduceCommand.OutputType.REPLACE,
					query
			);

			MapReduceOutput out = thingsCollection.mapReduce(cmd);

			tu.mark();

			System.out.println("Input  rows: " + out.getInputCount() );
			System.out.println("Emit   rows: " + out.getEmitCount() );
			System.out.println("Output rows: " + out.getOutputCount() );

			//for (DBObject o : out.results()) {
			//	System.out.println(o.toString());
			//}

			//timer
			System.out.println("map reduced in " + tu.getLastDelt() + " ms ");

		} catch (IOException e) {
			System.out.println(e.getCause());
		}
	}

	private HashMap<String,Object> httpPutMessage(String url, String body) throws IOException, URISyntaxException
	{
		CloseableHttpClient httpclient = HttpClients.createDefault();

		//HttpPost httppost = new HttpPost( url );
		HttpPut  http = new HttpPut(url);
		HashMap<String,Object> res = new HashMap<String,Object>();

		http.setHeader("Content-Type", "application/json");
		http.setHeader("Api_key", "root");

		StringEntity entity = new StringEntity( body, ContentType.create("text/plain", "UTF-8") );
		http.setEntity( entity );
		CloseableHttpResponse response = httpclient.execute( http );
		try
		{
			InputStream is = response.getEntity().getContent();
			InputStreamReader isr = new InputStreamReader( is );
			BufferedReader br = new BufferedReader( isr );
			String resp = "";
			String line;
			while( (line = br.readLine()) != null )
			{
				resp += line;
			}

			JsonFactory factory = new JsonFactory();
			ObjectMapper mapper = new ObjectMapper(factory);
			TypeReference<HashMap<String,Object>> typeRef
					= new TypeReference<HashMap<String,Object>>() {};

			try {
				res = mapper.readValue(resp, typeRef);
				System.out.println("Got " + res);
			} catch (Exception e) {

			}


		}
		finally
		{
			response.close();
			return res;
		}
	}

	private HashMap<String,Object> httpPatchMessage(String url, String body) throws IOException, URISyntaxException
	{
		CloseableHttpClient httpclient = HttpClients.createDefault();

		HttpPatch http = new HttpPatch(url);
		HashMap<String,Object> res = new HashMap<String,Object>();

		http.setHeader("Content-Type", "application/json");
		http.setHeader("Api_key", "root");

		StringEntity entity = new StringEntity( body, ContentType.create("text/plain", "UTF-8") );
		http.setEntity( entity );
		CloseableHttpResponse response = httpclient.execute( http );
		try
		{
			InputStream is = response.getEntity().getContent();
			InputStreamReader isr = new InputStreamReader( is );
			BufferedReader br = new BufferedReader( isr );
			String resp = "";
			String line;
			while( (line = br.readLine()) != null )
			{
				resp += line;
			}

			JsonFactory factory = new JsonFactory();
			ObjectMapper mapper = new ObjectMapper(factory);
			TypeReference<HashMap<String,Object>> typeRef
					= new TypeReference<HashMap<String,Object>>() {};

			res = mapper.readValue(resp, typeRef);
			System.out.println("Got " + res);

		}
		finally
		{
			response.close();
			return res;
		}
	}

	private HashMap<String,Object> httpPostMessage(String url, String body) throws IOException, URISyntaxException
	{
		CloseableHttpClient httpclient = HttpClients.createDefault();

		HttpPost http = new HttpPost(url);
		HashMap<String,Object> res = new HashMap<String,Object>();

		http.setHeader("Content-Type", "application/json");
		http.setHeader("Api_key", "root");

		StringEntity entity = new StringEntity( body, ContentType.create("text/plain", "UTF-8") );
		http.setEntity( entity );
		CloseableHttpResponse response = httpclient.execute( http );
		try
		{
			InputStream is = response.getEntity().getContent();
			InputStreamReader isr = new InputStreamReader( is );
			BufferedReader br = new BufferedReader( isr );
			String resp = "";
			String line;
			while( (line = br.readLine()) != null )
			{
				resp += line;
			}

			JsonFactory factory = new JsonFactory();
			ObjectMapper mapper = new ObjectMapper(factory);
			TypeReference<HashMap<String,Object>> typeRef
					= new TypeReference<HashMap<String,Object>>() {};

			res = mapper.readValue(resp, typeRef);
			//System.out.println("Got " + res);

		}
		finally
		{
			response.close();
			return res;
		}
	}

	public void createThingTypes() {
		HashMap<String,Object> res;
		try {

			String body = read("/forkliftParent.txt");
			res = httpPutMessage(cu.servicesApi + "thingType", body);
			Long parentId = Long.parseLong(res.get("id").toString());

			String children1 = read("/forkliftChildren1.txt");
			children1 = children1.replace("PARENT_ID", parentId.toString());
			httpPutMessage( cu.servicesApi + "thingType", children1 );

			String children2 = read("/forkliftChildren2.txt");
			children2 = children2.replaceAll("PARENT_ID", parentId.toString());
			httpPutMessage(cu.servicesApi + "thingType", children2);

		} catch (Exception e) {
			System.out.println(e.getCause());
		} finally
		{
			System.out.println("send tickle /v1/edge/dn/_ALL_/update/thingTypes");
			String topic = "/v1/edge/dn/_ALL_/update/thingTypes";
			cu.publishSyncMessage( topic, "" );

		}
	}

	private String castSerialNumber(Long n) {
		String serial = "000000000000000000000" + n;
		serial = serial.substring(serial.length()-21, serial.length());
		return serial;
	}

	private String nextSerialNumber() {
		serialNumber++;
		String serial = "000000000000000000000" + serialNumber;
		serial = serial.substring(serial.length()-21, serial.length());
		return serial;
	}

	public void createThings()
	{
		String tag;
		Integer delayBetweenThings = 1000;
		StringBuffer sb = new StringBuffer();
		HashMap<String, DBObject> stats = cu.getThingsPerThingType();
		if (stats.get("forklift") == null) {
			lastSerialNumber = "1";
		} else
		{
			lastSerialNumber = (Long.parseLong( stats.get( "forklift" ).get( "max" ).toString()) + 1) + "";
		}

		lastSerialNumber = "000000000000000000000" + cu.prompt( "enter Starting serialNumber", lastSerialNumber );

		lastSerialNumber = lastSerialNumber.substring( lastSerialNumber.length() - 21, lastSerialNumber.length() );
		serialNumber = Long.parseLong( lastSerialNumber);

		//quantity
		lastQuantity = cu.prompt( "enter number of things to create", "" + lastQuantity );
		Long quantity = Long.parseLong( lastQuantity);

		//thingsPerMessage
		lastThingsPerMessage = cu.prompt( "Things per Message", "" + lastThingsPerMessage );
		Long thingsPerMessage = Long.parseLong( lastThingsPerMessage);

		//delayBetweenThings = Integer.parseInt( cu.prompt( "How many miliseconds (ms) between each blink ?", "" + delayBetweenThings ));
		delayBetweenThings = 1500;
		errores = 0L;
		created = 0L;
		TimerUtils tu = new TimerUtils();
		tu.mark();

		for (Long i=0L; i < quantity/3/thingsPerMessage; i++) {
			createParentChildThings( delayBetweenThings, thingsPerMessage );
			tu.mark();
			System.out.println("      created:" + created + " time:" + tu.getLastDelt() + " ms.");
		}
		System.out.println("TOTAL created:" + created + " time:" + tu.getTotalDelt() + " ms.");

	}

	private String getRandomBrand()
	{
		Random rnd = new Random();
		String[] options = {"Toyota", "Mitsubishi", "John Deere", "Nissan", "Volkswagen", "GM", "Chrysler"};

		Integer r = rnd.nextInt(options.length);

		return options[r];
	}

	private String getRandomStatus()
	{
		Random rnd = new Random();
		String[] options = {"active", "transit", "maintenance", "out_of_service", "inventory"};

		Integer r = rnd.nextInt(options.length);

		return options[r];
	}

	private String getRandomUsage()
	{
		Random rnd = new Random();
		Integer r = rnd.nextInt(10000);

		return "" + r / 100.0;
	}


	private Long getIdFromThing(String thingTypeCode, String serialNumber)
	{
		int times = 30;

		do
		{
			BasicDBObject query = new BasicDBObject( "serialNumber", serialNumber ).append( "thingTypeCode", thingTypeCode );
			DBObject p = thingsCollection.findOne( query );
			if( p == null )
			{
				System.out.print( "." );
				cu.sleep( times*100 );
				times--;
				if (times < 0) {
					return null;
				}
			}
			else
			{
				System.out.println("    " + p.get("_id").toString() + " = " + serialNumber + " " + thingTypeCode );
				return Long.valueOf( p.get( "_id" ).toString() );
			}
		} while (times >= 0);
		return null;
	}

	public void createParentChildThings(Integer delayBetweenThings, Long thingsPerMessage)
	{
		HashMap<String,Object> res;

		try {
			String topic, msg, serial1, serial2, serial3, lr;
			Integer posx, posy;
			Random r = new Random();

			Long time = new Date().getTime();
			Integer i;

			Long baseSerial = serialNumber;


			//the parent thing : forklift
			serialNumber = baseSerial - 1;
			topic = "/v1/data/ALEB/forklift";
			sequenceNumber = cu.getSequenceNumber();
			msg = " sn," + sequenceNumber + "\n";
			msg += ",0,___CS___,-118.443969;34.048092;0.0;20.0;ft\n";
			for (i=0; i<thingsPerMessage; i++) {
				serial1 = nextSerialNumber();

				msg += serial1 + "," + time + ",lastDetectTime,1436985931348\n";
				msg += serial1 + "," + time + ",brand," + getRandomBrand() + "\n";
				msg += serial1 + "," + time + ",status," + getRandomStatus() + "\n";
				msg += serial1 + "," + time + ",usage," + getRandomUsage() + "\n";
			}
			cu.publishSyncMessage(topic, msg);
			cu.sleep(100 );

			//the first thing
			serialNumber = baseSerial - 1;
			topic = "/v1/data/ALEB/forkliftBattery";
			sequenceNumber = cu.getSequenceNumber();
			msg = " sn," + sequenceNumber + "\n";
			msg += ",0,___CS___,-118.443969;34.048092;0.0;20.0;ft\n";
			for (i=0; i<thingsPerMessage; i++) {
				serial2 = nextSerialNumber();
				posx = r.nextInt(499);
				posy = r.nextInt(499);
				lr = "LR" + r.nextInt(10);

				msg += serial2 + "," + time + ",location,-118.44395517462448;34.04811656588989;0.0\n";
				msg += serial2 + "," + time + ",locationXYZ," + posx + ".0;" + posy + ".0;0.0\n";
				msg += serial2 + "," + time + ",logicalReader," + lr + "\n";
				msg += serial2 + "," + time + ",lastLocateTime,1436985931348\n";
				msg += serial2 + "," + time + ",lastDetectTime,1436985931348\n";
			}
			cu.publishSyncMessage(topic, msg);
			cu.sleep( 100 );

			//the third thing
			serialNumber = baseSerial - 1;
			topic = "/v1/data/ALEB/forkliftSolar";
			sequenceNumber = cu.getSequenceNumber();
			msg = " sn," + sequenceNumber + "\n";
			msg += ",0,___CS___,-118.443969;34.048092;0.0;20.0;ft\n";
			for (i=0; i<thingsPerMessage; i++) {
				serial3 = nextSerialNumber();
				posx = r.nextInt(499);
				posy = r.nextInt(499);
				lr = "LR" + r.nextInt(10);
				msg += serial3 + "," + time + ",location,-118.44395517462448;34.04811656588989;0.0\n";
				msg += serial3 + "," + time + ",locationXYZ," + posx + ".0;" + posy + ".0;0.0\n";
				msg += serial3 + "," + time + ",logicalReader," + lr + "\n";
				msg += serial3 + "," + time + ",lastLocateTime,1436985931348\n";
				msg += serial3 + "," + time + ",lastDetectTime,1436985931348\n";
			}
			cu.publishSyncMessage(topic, msg);
			cu.sleep(100);

			created += thingsPerMessage*3;

			//update parent for forkliftBattery
			String url;
			for (i=0; i<thingsPerMessage; i++) {
				try {

					String parentId = "" + getIdFromThing("forklift", castSerialNumber(baseSerial +  i) );
					getIdFromThing("forkliftBattery", castSerialNumber(baseSerial +  i) );
					getIdFromThing("forkliftSolar",   castSerialNumber(baseSerial +  i) );

					StringBuilder sb = new StringBuilder( "" );
					sb.append( "{ \"serialNumber\": \"" + castSerialNumber( baseSerial + i) + "\", ");
					sb.append( "\"thingTypeCode\": \"forklift\", " );
					sb.append( "\"groupName\" : \">mojix>SM\", " );
					sb.append( "\"name\" : \"" + castSerialNumber( baseSerial + i) +"\", " );
					sb.append( "\"children\": [ " );
					sb.append( "{" );
					sb.append( "\"thingTypeCode\" : \"forkliftBattery\", ");
					sb.append( "\"serialNumber\" : \"" + castSerialNumber( baseSerial + i) + "\"" );
					sb.append( " },");
					sb.append( "{" );
					sb.append( "\"thingTypeCode\" : \"forkliftSolar\", ");
					sb.append( "\"serialNumber\" : \"" + castSerialNumber( baseSerial  + i) + "\"" );
					sb.append( " }");
					sb.append( " ] } ");

					url = "http://localhost:8080/riot-core-services/api/thing/" + parentId;
					res = httpPatchMessage( url, sb.toString() );
					System.out.println( res );

				} catch (Exception e) {
					System.out.println(e.getCause());
				}
			}
		} catch (Exception e) {
			System.out.println(e.getCause());
		}
	}

	private void sendChangeMessage( String serialNumber, String thingType, Integer delayBetweenThings)
	{
		sequenceNumber = cu.getSequenceNumber();

		String topic = "/v1/data/ALEB/" + thingType;
		StringBuffer msg = new StringBuffer();
		msg.append(" sn," + sequenceNumber + "\n");
		msg.append(",0,___CS___,-118.443969;34.048092;0.0;20.0;ft\n");
		Long time = new Date().getTime();
		Random r = new Random();

		if (thingType.equals("forklift")) {
			msg.append(serialNumber + "," + time + ",lastDetectTime," + time + "\n");
			msg.append(serialNumber + "," + time + ",brand," + getRandomBrand() + "\n");
			msg.append(serialNumber + "," + time + ",status," + getRandomStatus() + "\n");
			msg.append(serialNumber + "," + time + ",usage," + getRandomUsage() + "\n");
		} else  {
			msg.append(serialNumber + "," + time + ",location,-118.44395517462448;34.04811656588989;0.0\n");
			msg.append(serialNumber + "," + time + ",locationXYZ," + r.nextInt(499) + ".0;" + r.nextInt(499) + ".0;0.0\n");
			msg.append(serialNumber + "," + time + ",logicalReader,LR" + r.nextInt(10) + "\n");
			msg.append(serialNumber + "," + time + ",lastDetectTime," + time + "\n");
		}
		cu.publishSyncMessage(topic, msg.toString());
		if( delayBetweenThings > 0)
		{
			cu.sleep( delayBetweenThings );
		}
	}

	private void changeThings( Boolean allThings )
	{
		String tag;
		StringBuffer sb = new StringBuffer();
		Scanner in;
		in = new Scanner(System.in);
		Integer delayBetweenThings = 800;
		Integer thingsPerMessage = 400;

		thingsPerMessage = Integer.parseInt( cu.prompt( "How many things in each blink?", "" + thingsPerMessage ) );

		Long timesBlink = thingsToChange / thingsPerMessage;
		timesBlink = Long.parseLong( cu.prompt( "How many times sends the blink?", "" + timesBlink ) );
		thingsToChange = timesBlink * thingsPerMessage;

		delayBetweenThings = Integer.parseInt( cu.prompt( "How many ms beetween each blink?", "" + delayBetweenThings ) );

		System.out.print(cu.black() + "\nChanging " + thingsToChange + " things with a delay of " + delayBetweenThings + " ms.");

		//get the max number of _id
		Long maxId = 0L;
		String serialNumber;
		String thingType;
		Random random = new Random();

		DBObject sortby = new BasicDBObject("_id", -1);
		DBCursor cursor = thingsCollection.find().sort(sortby).limit(1);
		try {
			if (cursor.hasNext()) {
				cursor.next();
				maxId = Long.parseLong(cursor.curr().get("_id").toString());
			}
		} finally {
			cursor.close();
		}
		if (maxId == 0 ) {
			System.out.println("error or zero documents in collection 'things' ");
			return;
		}

		System.out.println("The max value for _id is " + maxId);

		String thingTypeCode= "forkliftBattery";
		for (Integer i = 0; i < thingsToChange / thingsPerMessage; i ++ )
		{
			//select the thingtype
			switch( random.nextInt( 2 ) )
			{
				case 0:
					if (allThings)
					{
						thingTypeCode = "forklift";
					} else {
						thingTypeCode = "forkliftBattery";
					}
					break;
				case 1:
					thingTypeCode = "forkliftSolar";
					break;
				case 2:
					thingTypeCode = "forkliftBattery";
					break;
			}

			//start building the message
			serialNumber = cursor.curr().get("serialNumber").toString();
			sequenceNumber = cu.getSequenceNumber();

			String topic = "/v1/data/ALEB/" + thingTypeCode;
			StringBuffer msg = new StringBuffer();
			msg.append(" sn," + sequenceNumber + "\n");
			msg.append(",0,___CS___,-118.443969;34.048092;0.0;20.0;ft\n");
			Long time = new Date().getTime();

			System.out.println ("\nsn:" + sequenceNumber + " " + thingTypeCode + "  " + thingsPerMessage + " things per msg");
			//select a random object n times
			for (int j = 0; j < thingsPerMessage; )
			{
				DBObject filterById = new BasicDBObject( "_id", random.nextLong() % maxId );
				cursor = thingsCollection.find( filterById ).limit( 1 );
				try
				{
					if( cursor.hasNext() )
					{
						cursor.next();
						serialNumber = cursor.curr().get("serialNumber").toString();
						thingType    = cursor.curr().get("thingTypeCode").toString();
						Random r = new Random();

						if ( thingType.equals( thingTypeCode) )
						{
							if( thingTypeCode.equals( "forklift" ) )
							{
								msg.append( serialNumber + "," + time + ",lastDetectTime," + time + "\n" );
								msg.append( serialNumber + "," + time + ",brand," + getRandomBrand() + "\n" );
								msg.append( serialNumber + "," + time + ",status," + getRandomStatus() + "\n" );
								msg.append( serialNumber + "," + time + ",usage," + getRandomUsage() + "\n" );
							}
							else
							{
								msg.append( serialNumber + "," + time + ",location,-118.44395517462448;34.04811656588989;0.0\n" );
								msg.append( serialNumber + "," + time + ",locationXYZ," + r.nextInt( 499 ) + ".0;" + r.nextInt( 499 ) + ".0;0.0\n" );
								msg.append( serialNumber + "," + time + ",logicalReader,LR" + r.nextInt( 10 ) + "\n" );
								msg.append( serialNumber + "," + time + ",lastDetectTime," + time + "\n" );
							}

							j++;
							if ( j < 107)
							{
								System.out.print( cu.alignRight( serialNumber, 6 ) + " " );
								if( j % 15 == 0 )
								{
									System.out.println( "" );
								}
							} else {
								if( j % 25 == 0 )
								{
									System.out.print( "." );
								}
							}
						}
					}
				}
				finally
				{
					cursor.close();
				}
			}
			cu.publishSyncMessage( topic, msg.toString() );
			cu.sleep( delayBetweenThings );

		}

	}

	private void changeOneThing()
	{
		StringBuffer sb = new StringBuffer();
		Scanner in;
		in = new Scanner(System.in);
		Long thingsToChange = 0L;
		Integer delayBetweenThings = 10;

		System.out.print(cu.black() + "\nserialNumber[" + cu.green() + lastSerialNumber + cu.black() + "]:");
		String tagIn = in.nextLine();
		if (tagIn.equals("")) {
			tagIn = lastSerialNumber;
		} else {
			tagIn = "" + tagIn;
		}

		String serialNumber = castSerialNumber(Long.parseLong( tagIn ));
        lastSerialNumber = serialNumber;

		System.out.print(cu.black() + "\nChanging the thing " + serialNumber );

		//get the max number of _id
		Long maxId = 0L;
		String thingType = "";

		DBObject query = new BasicDBObject("serialNumber", serialNumber);
		DBCursor cursor = thingsCollection.find( query );
		try {
			if (cursor.hasNext()) {
				cursor.next();
				thingType = cursor.curr().get("thingTypeCode").toString();
			}
		} finally {
			cursor.close();
		}
		if (thingType.equals( "" ) ) {
			System.out.println("error or zero documents in collection 'things' ");
			return;
		}

		sendChangeMessage( serialNumber, thingType, 0 );

	}

	public void execute() {
		setup();
		HashMap<String, String> options = new HashMap<String,String>();

		options.put("1", "create ThingTypes ");
		options.put("2", "create Parent-Child things");
		options.put("3", "change things ");
		options.put("4", "change child things only");
		options.put("5", "change 1 things");

		Integer option = 0;
		while (option != null) {
			option = cu.showMenu("parent children options", options );
			if (option != null) {
				if (option == 0) {
					createThingTypes();
				}
				if (option == 1) {
					createThings();
				}
				if (option == 2) {
					changeThings(true);
				}
				if (option == 3) {
					changeThings(false);
				}
				if (option == 4) {
					changeOneThing();
				}

				System.out.println(cu.black() +  "\npress [enter] to continue");
				Scanner in = new Scanner(System.in);
				in.nextLine();
			}
		}

	}

}
