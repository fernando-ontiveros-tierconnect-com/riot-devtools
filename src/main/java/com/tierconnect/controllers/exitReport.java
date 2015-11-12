package com.tierconnect.controllers;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.tierconnect.dev.controllerInterface;
import com.tierconnect.utils.CommonUtils;
import com.tierconnect.utils.TimerUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Random;
import java.util.Scanner;

/**
 * Created by fernando on 7/17/15.
 */
public class exitReport implements controllerInterface
{
	CommonUtils cu;
	String lastSerialNumber = "000000000000000010000";
	String lastQuantity = "10000";
	String lastThingsPerMessage = "100";
	ArrayList<Long> validIds;
	ArrayList<String> validSerialNumbers;
	Integer lastPosy = 0;

	Long sequenceNumber = 0L;
	Long serialNumber = 200L;
	Long errores = 0L;
	Long created = 0L;

	Long thingsToChange = 40000L;

	DBCollection thingsCollection;
	DBCollection exitCollection;
	BasicDBObject docs[];		String tag;

	String thingTypeCode   = "default_rfid_thingtype";
	String parentThingType = "assets";
	String thingField    = "status";
	String thingFieldJSON = "shifts";

	public void setCu(CommonUtils cu) {
		this.cu = cu;
	}


	public String getDescription() {

		return "Exit Report example";
	}

	public void setup()
	{
		thingsCollection        = cu.db.getCollection("things");
		exitCollection        = cu.db.getCollection("exit_report");

		cu.defaultMqttConnection();

	}

	private String read( String fname ) throws IOException
	{
		InputStream is = exitReport.class.getResourceAsStream( fname );
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


	public void createThingTypes() {
		cu.createThingTypeFromFile( "/assets.txt" );
	}

	private String castSerialNumber(Long n) {
		String serial = "000000000000000000000" + n;
		serial = serial.substring(serial.length()-21, serial.length());
		return serial;
	}

	private String castAssetsSerialNumber( Long n ) {
		String serial = "000000000000000000000" + n;
		serial = "assets" + serial.substring(serial.length()-15, serial.length());
		return serial;
	}

	private String nextAssetsSerialNumber() {
		serialNumber++;
		String serial = "000000000000000000000" + serialNumber;
		serial = "assets" + serial.substring(serial.length()-15, serial.length());
		return serial;
	}

	private String nextSerialNumber() {
		serialNumber++;
		String serial = "000000000000000000000" + serialNumber;
		serial = serial.substring(serial.length()-21, serial.length());
		return serial;
	}

	private HashMap<String,Object> httpPatchMessage(String url, String body) throws IOException, URISyntaxException
	{
		CloseableHttpClient httpclient = HttpClients.createDefault();

		HttpPatch http = new HttpPatch(url);
		HashMap<String,Object> res = new HashMap<String,Object>();

		http.setHeader("Content-Type", "application/json");
		http.setHeader("Api_key", "root");

		StringEntity entity = new StringEntity( body, ContentType.create( "text/plain", "UTF-8" ) );
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


	public void createThings()
	{
		Integer delayBetweenThings = 1000;
		StringBuffer sb = new StringBuffer();
		HashMap<String, DBObject> stats = cu.getThingsPerThingType();
		if (stats.get( thingTypeCode ) == null) {
			lastSerialNumber = "1";
		} else
		{
			try
			{
				lastSerialNumber = (Long.parseLong( stats.get( thingTypeCode ).get( "max" ).toString() ) + 1) + "";
			} catch( Exception e ) {
				lastSerialNumber = (Long.parseLong( stats.get( thingTypeCode ).get( "count" ).toString() ) + 1) + "";
			}
		}

		lastSerialNumber = "000000000000000000000" + cu.prompt( "enter Starting serialNumber", lastSerialNumber );

		lastSerialNumber = lastSerialNumber.substring( lastSerialNumber.length() - 21, lastSerialNumber.length() );
		serialNumber = Long.parseLong( lastSerialNumber);

		//quantity
		lastQuantity = cu.prompt( "enter number of pair things to create", "" + lastQuantity );
		Long quantity = Long.parseLong( lastQuantity);

		//thingsPerMessage
		lastThingsPerMessage = cu.prompt( "Things per Message", "" + lastThingsPerMessage );
		Long thingsPerMessage = Long.parseLong( lastThingsPerMessage);
		if (thingsPerMessage > quantity) {
			thingsPerMessage = quantity;
		}

		delayBetweenThings = Integer.parseInt( cu.prompt( "How many miliseconds (ms) between each blink ?", "" + delayBetweenThings ));

		errores = 0L;
		created = 0L;
		TimerUtils tu = new TimerUtils();
		tu.mark();

		for (Long i=0L; i < quantity/thingsPerMessage; i++) {
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

	private String getRandomCategory()
	{
		Random rnd = new Random();
		String[] options = {"active", "transit", "maintenance", "out_of_service", "inventory"};

		Integer r = rnd.nextInt(options.length);

		return options[r];
	}

	private String getRandomColor()
	{
		Random rnd = new Random();
		String[] options = {"red", "blue", "yellow", "brown", "cyan", "black", "white", "dark blue", "magenta"};

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

	private String getRandomPrice()
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
			String topic, msg, serial1, serial2;
			String locx, locy;
			Random r = new Random();

			Long time = new Date().getTime();
			Integer i;

			Long baseSerial = serialNumber;


			//the parent thing : assets
			serialNumber = baseSerial - 1;
			topic = "/v1/data/ALEB/" + parentThingType;
			sequenceNumber = cu.getSequenceNumber();
			msg = " sn," + sequenceNumber + "\n";
			msg += ",0,___CS___,-118.443969;34.048092;0.0;20.0;ft\n";
			serial1 = "";
			serial2 = "";
			for (i=0; i<thingsPerMessage; i++) {
				serial1 = nextAssetsSerialNumber();

				msg += serial1 + "," + time + ",lastDetectTime,1436985931348\n";
				msg += serial1 + "," + time + ",brand," + getRandomBrand() + "\n";
				msg += serial1 + "," + time + ",category," + getRandomCategory() + "\n";
				msg += serial1 + "," + time + ",status," + getRandomStatus() + "\n";
				msg += serial1 + "," + time + ",color," + getRandomColor() + "\n";
				msg += serial1 + "," + time + ",price," + getRandomPrice() + "\n";
				msg += serial1 + "," + time + ",usage," + getRandomPrice() + "\n";
			}
			cu.publishSyncMessage(topic, msg);
			cu.sleep(300 );

			//the first thing
			serialNumber = baseSerial - 1;
			time = new Date().getTime();
			topic = "/v1/data/ALEB/" + thingTypeCode;
			sequenceNumber = cu.getSequenceNumber();
			msg = " sn," + sequenceNumber + "\n";
			msg += ",0,___CS___,-118.443969;34.048092;0.0;20.0;ft\n";
			for (i=0; i<thingsPerMessage; i++) {
				serial2 = nextSerialNumber();
				locx = "-118.44394" + r.nextInt(99);
				locy = "34.04825" + r.nextInt(99);

				msg += serial2 + "," + time + ",location," + locx + ";" + locy + ";0.0\n";
				//msg += serial2 + "," + time + ",locationXYZ," + posx + ".0;" + posy + ".0;0.0\n";
				msg += serial2 + "," + time + ",lastLocateTime," + time + "\n";
				msg += serial2 + "," + time + ",lastDetectTime," + time + "\n";
			}
			cu.publishSyncMessage(topic, msg);
			cu.sleep( 300 );


			created += thingsPerMessage*3;

			//update parent for default_rfid_thingtype
			String url;
			for (i=0; i<thingsPerMessage; i++) {
				try {
					serial1 = castAssetsSerialNumber( baseSerial + i ) ;
					serial2 = castSerialNumber(baseSerial +  i);
					Long parentId = getIdFromThing(parentThingType, serial1 );
					Long childId  = getIdFromThing(thingTypeCode  , serial2 );

					StringBuilder sb = new StringBuilder( "" );
					sb.append( "{\"group\":\">mojix>SM\",");
					sb.append( "\"parent\":{");
					sb.append( "\"serialNumber\":\"" + serial1 +"\",");
					sb.append( "\"thingTypeCode\":\"assets\",");
					sb.append( "\"id\":" + parentId);
					sb.append( "},");
					sb.append( "\"name\":\"" + serial2 + "\",");
					sb.append( "\"thingTypeCode\":\"default_rfid_thingtype\",");
					sb.append( "\"udfs\":{},");
					sb.append( "\"serialNumber\":\"" + serial2 + "\"}");


					url = "http://localhost:8080/riot-core-services/api/thing/" + childId;

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

	private void changeThings( )
	{
		validIds = new ArrayList<>();
		validSerialNumbers = new ArrayList<>();

		DBCollection thingsCollection = cu.db.getCollection( "things" );
		BasicDBObject queryDoc  = new BasicDBObject( "thingTypeCode", parentThingType).
				append( "children.thingTypeCode", thingTypeCode );
		BasicDBObject fieldsDoc = new BasicDBObject( "_id" , 1 ).append( "serialNumber", 1 ).append( "children", 1);

		DBCursor cursor = thingsCollection.find(queryDoc, fieldsDoc);
		while (cursor.hasNext()) {
			BasicDBObject doc = (BasicDBObject)cursor.next();
			BasicDBList childrenList = (BasicDBList)doc.get("children");
			BasicDBObject childrenObject = (BasicDBObject)childrenList.get( 0 );

			validIds.add( Long.parseLong( childrenObject.get("_id").toString() ));
			validSerialNumbers.add( childrenObject.get("serialNumber").toString() );
		}

		if (validIds.size() == 0 ) {
			System.out.println("error or zero documents in collection 'things' ");
			return;
		}

		Integer delayBetweenThings = 1000;
		Integer timesBlink = 100;

		timesBlink = Integer.parseInt( cu.prompt( "How many things to change?", "" + timesBlink ) );

		delayBetweenThings = Integer.parseInt( cu.prompt( "How many ms beetween each blink?", "" + delayBetweenThings ) );

		System.out.println(
				cu.black() + "\nChanging " + timesBlink + " default_rfid things with a delay of " + delayBetweenThings + " ms." );

		Long maxId = 0L;
		Long thingId;
		String serialNumber;
		String thingType;
		Random random = new Random();

		System.out.println("There are " + validIds.size() + " valid things to change");

		//String thingTypeCode= "default_rfid_thingtype";
		for (Integer i = 0; i < timesBlink; i ++ )
		{
			//start building the message
			int rindex = random.nextInt(validSerialNumbers.size());
			thingId = validIds.get( rindex );
			serialNumber = validSerialNumbers.get( rindex ).toString();
			sequenceNumber = cu.getSequenceNumber();

			String topic = "/v1/data/ALEB/" + thingTypeCode;
			StringBuffer msg = new StringBuffer();
			msg.append(" sn," + sequenceNumber + "\n");
			msg.append(",0,___CS___,-118.443969;34.048092;0.0;20.0;ft\n");
			Long time = new Date().getTime();

			String zoneName = "";
			String location = "";

			switch( random.nextInt( 8 ) ) {
			//switch( random.nextInt( 2 ) +6 ) {
				case 0: zoneName = "gate1";
					location = "-118.44395;34.04792";
					break;
				case 1: zoneName = "gate2";
					location = "-118.44389;34.047941";
					break;
				case 2: zoneName = "gate3";
					location = "-118.44382;34.04796";
					break;
				case 3: zoneName = "gate4";
					location = "-118.44377;34.04798";
					break;
				case 4: zoneName = "gate5";
					location = "-118.44371;34.04800";
					break;
				case 5: zoneName = "gate6";
					location = "-118.44365;34.04802";
					break;
				case 6: zoneName = "Stockroom";
					location = "-118.44392;34.04826";
					break;
				case 7: zoneName = "Salesfloor";
					location = "-118.44379;34.04823";
					break;
			}

			System.out.println ("sn:" + sequenceNumber + " " + thingTypeCode + "  " + serialNumber + "(" + thingId + ")" + " moved to zone " + zoneName);
			//select a random object n times
			//msg.append( serialNumber + "," + time + ",brand," + getRandomBrand() + "\n" );
			//msg.append( serialNumber + "," + time + ",status," + getRandomStatus() + "\n" );
			//msg.append( serialNumber + "," + time + ",usage," + getRandomUsage() + "\n" );

			//msg.append( serialNumber + "," + time + ",zone," + randomZone+ ";0.0\n" );
			msg.append( serialNumber + "," + time + ",location," + location + ";0.0\n" );
			//msg.append( serialNumber + "," + time + ",locationXYZ,-17.0;-59.0;0.0\n" );
			msg.append( serialNumber + "," + time + ",lastDetectTime," + time + "\n" );

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
		HashMap<String, String> options = new LinkedHashMap<String,String>();

		options.put("1", "create Assets Thing Type");
		options.put("2", "create Assets and RFIds tags");
		options.put("3", "change things ");

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
					changeThings();
				}

				System.out.println(cu.black() +  "\npress [enter] to continue");
				Scanner in = new Scanner(System.in);
				in.nextLine();
			}
		}

	}

}
