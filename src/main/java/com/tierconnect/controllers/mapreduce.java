package com.tierconnect.controllers;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MapReduceCommand;
import com.mongodb.MapReduceOutput;
import com.tierconnect.dev.controllerInterface;
import com.tierconnect.utils.CommonUtils;
import com.tierconnect.utils.MqttUtils;
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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Random;
import java.util.Scanner;

/**
 * Created by fernando on 7/17/15.
 */
public class mapreduce implements controllerInterface
{
	CommonUtils cu;
	String lastSerialNumber = "000000000000000010000";
	String lastQuantity = "10000";
	Integer lastPosx = 0;
	Integer lastPosy = 0;

	Long sequenceNumber = 0L;
	Long serialNumber = 200L;
	Long errores = 0L;
	Long created = 0L;

	DBCollection thingsCollection;
	DBCollection outputCollection;
	BasicDBObject docs[];		String tag;

	String thingTypeCode = "default_rfid_thingtype";
	String thingField    = "status";
	String thingFieldJSON = "shifts";

	MqttUtils mq;

	public void setCu(CommonUtils cu) {
		this.cu = cu;
	}


	public String getDescription() {

		return "map reduce functions";
	}

	public void setup()
	{
		thingsCollection        = cu.db.getCollection("things");
		outputCollection        = cu.db.getCollection("mr_reusableTag");

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
			res = httpPutMessage("http://localhost:8080/riot-core-services/api/thingType", body);
			Long parentId = Long.parseLong(res.get("id").toString());

			String children1 = read("/forkliftChildren1.txt");
			children1 = children1.replace("PARENT_ID", parentId.toString());
			httpPutMessage( "http://localhost:8080/riot-core-services/api/thingType", children1 );

			String children2 = read("/forkliftChildren2.txt");
			children2 = children2.replaceAll("PARENT_ID", parentId.toString());
			httpPutMessage("http://localhost:8080/riot-core-services/api/thingType", children2);

		} catch (Exception e) {
			System.out.println(e.getCause());
		} finally
		{
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
		Scanner in;
		in = new Scanner(System.in);

		System.out.print(cu.black() + "\nenter the starting serialNumber[" + cu.green() + lastSerialNumber + cu.black() + "]:");
		String tagIn = in.nextLine();
		if (tagIn.equals("")) {
			tagIn = lastSerialNumber;
		} else {
			tagIn = "000000000000000000000" + tagIn;
		}
		tag = tagIn.substring(tagIn.length()-21, tagIn.length());
		lastSerialNumber = tag;
		serialNumber = Long.parseLong( tag);

		//quantity
		System.out.print(cu.black() + "\nenter the quantity of things to create[" + cu.green() + lastQuantity + cu.black() + "]:");
		tagIn = in.nextLine();
		if (tagIn.equals("")) {
			tagIn = lastQuantity;
		} else {
			tagIn = "0" + tagIn;
		}
		lastQuantity = tagIn;
		Long quantity = Long.parseLong( lastQuantity);

		System.out.print(cu.black() + "\nHow many miliseconds (ms) between each blink ?[" + cu.green() + delayBetweenThings + cu.black() + "]:");
		tagIn = in.nextLine();
		if (tagIn.equals("")) {
			//delayBetweenThings = delayBetweenThings;
		} else {
			delayBetweenThings = Integer.parseInt( tagIn );
		}

		errores = 0L;
		created = 0L;
		TimerUtils tu = new TimerUtils();
		tu.mark();

		for (Long i=0L; i < quantity/3/100; i++) {
			//createOneThing();
			createHundredThings(delayBetweenThings);
			tu.mark();
			System.out.println("      created:" + created + "  errores:" + errores + " time:" + tu.getLastDelt() + " ms.");
		}
		System.out.println("TOTAL created:" + created + "  errores:" + errores + " time:" + tu.getTotalDelt() + " ms.");

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

	public void createOneThing()
	{
		HashMap<String,Object> res;

		try {
			String topic, msg, serial1, serial2, serial3, lr;
			Integer posx, posy;
			Random r = new Random();

			Long time = new Date().getTime();

			//the parent thing : forklift
			topic = "/v1/data/ALEB/forklift";
			sequenceNumber++;
			serial1 = nextSerialNumber();
			msg = " sn," + sequenceNumber + "\n";
			msg += ",0,___CS___,-118.443969;34.048092;0.0;20.0;ft\n";
			msg += serial1 + "," + time + ",lastDetectTime,1436985931348\n";
			msg += serial1 + "," + time + ",brand,"  + getRandomBrand()+ "\n";
			msg += serial1 + "," + time + ",status," + getRandomStatus()+ "\n";
			msg += serial1 + "," + time + ",usage,"  + getRandomUsage()+ "\n";
			mq.publishSyncMessage(topic, msg);
			//cu.sleep(10 );

			//the first thing
			topic = "/v1/data/ALEB/forkliftBattery";
			serial2 = nextSerialNumber();
			sequenceNumber++;
			posx = r.nextInt(499);
			posy = r.nextInt(499);
			lr = "LR" + r.nextInt(10);

			msg = " sn," + sequenceNumber + "\n";
			msg += serial2 + "," + time + ",location,-118.44395517462448;34.04811656588989;0.0\n";
			msg += serial2 + "," + time + ",locationXYZ," + posx + ".0;" + posy + ".0;0.0\n";
			msg += serial2 + "," + time + ",logicalReader," + lr + "\n";
			msg += serial2 + "," + time + ",lastLocateTime,1436985931348\n";
			msg += serial2 + "," + time + ",lastDetectTime,1436985931348\n";
			mq.publishSyncMessage(topic, msg);
			//cu.sleep(10 );

			//the third thing
			topic = "/v1/data/ALEB/forkliftSolar";
			serial3 = nextSerialNumber();
			sequenceNumber++;
			msg = " sn," + sequenceNumber + "\n";
			msg += serial3 + "," + time + ",location,-118.44395517462448;34.04811656588989;0.0\n";
			msg += serial3 + "," + time + ",locationXYZ," + posx + ".0;" + posy + ".0;0.0\n";
			msg += serial3 + "," + time + ",logicalReader," + lr + "\n";
			msg += serial3 + "," + time + ",lastLocateTime,1436985931348\n";
			msg += serial3 + "," + time + ",lastDetectTime,1436985931348\n";
//			msg += serial3 + ",1436985931348,status,status" + r.nextInt(10)+ "\n";
			mq.publishSyncMessage(topic, msg);
			cu.sleep(1 );

			String url;

			Boolean repeat;
			Integer times;  //this is a patch, because the CoreBridge has an error

			repeat = true;
			times = 10;
			while (repeat) {
				try {
					cu.sleep(1+(10-times)*25 );
					url = "http://localhost:8080/riot-core-services/api/thing/" + serial2 + "/setParent/" + serial1;
					res = httpPostMessage(url, "");
					if (res.get("modifiedTime") != null) {
						repeat = false;
					}
					times --; //retry only 10 times, and then continue
					if (times <= 0) {
						repeat = false;
					}
				} catch (Exception e) {
					System.out.println(e.getCause());
				}
			}

			repeat = true;
			times = 10;
			while (repeat) {
				try {
					url = "http://localhost:8080/riot-core-services/api/thing/" + serial3 + "/setParent/" + serial1;
					res = httpPostMessage(url, "");
					cu.sleep(1+(10-times)*10 );
					if (res.get("modifiedTime") != null) {
						repeat = false;
					}
					times --; //retry only 10 times, and then continue
					if (times <= 0) {
						repeat = false;
					}
				} catch (Exception e) {
					System.out.println(e.getCause());
				}
			}
/*
		Integer parentId = 57;
		Integer childrenId = 58;

		msg = "{\"group.id\":3,\"parent.id\":" +
				parentId + ",\"name\":\"" +
				serial + "\",\"serial\":\"" +
				serial + "\"}";

		httpPatchMessage("http://localhost:8080/riot-core-services/api/thing/" + childrenId, msg);

		childrenId = 59;
		httpPatchMessage("http://localhost:8080/riot-core-services/api/thing/" + childrenId, msg);
*/
		} catch (Exception e) {
			System.out.println(e.getCause());
		}
	}

	public void createHundredThings(Integer delayBetweenThings)
	{
		HashMap<String,Object> res;

		try {
			String topic, msg, serial1, serial2, serial3, lr;
			Integer posx, posy;
			Random r = new Random();

			Long time = new Date().getTime();
			Integer i;
			Long baseSerial;

			baseSerial = serialNumber +1;
			//the parent thing : forklift
			topic = "/v1/data/ALEB/forklift";
			sequenceNumber++;
			msg = " sn," + sequenceNumber + "\n";
			msg += ",0,___CS___,-118.443969;34.048092;0.0;20.0;ft\n";
			for (i=0; i<100; i++) {
				serial1 = nextSerialNumber();

				msg += serial1 + "," + time + ",lastDetectTime,1436985931348\n";
				msg += serial1 + "," + time + ",brand," + getRandomBrand() + "\n";
				msg += serial1 + "," + time + ",status," + getRandomStatus() + "\n";
				msg += serial1 + "," + time + ",usage," + getRandomUsage() + "\n";
			}
			mq.publishSyncMessage(topic, msg);
			//cu.sleep(10 );

			//the first thing
			topic = "/v1/data/ALEB/forkliftBattery";
			sequenceNumber++;
			msg = " sn," + sequenceNumber + "\n";
			msg += ",0,___CS___,-118.443969;34.048092;0.0;20.0;ft\n";
			for (i=0; i<100; i++) {
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
			mq.publishSyncMessage(topic, msg);
			//cu.sleep(10 );

			//the third thing
			topic = "/v1/data/ALEB/forkliftSolar";
			sequenceNumber++;
			msg = " sn," + sequenceNumber + "\n";
			msg += ",0,___CS___,-118.443969;34.048092;0.0;20.0;ft\n";
			for (i=0; i<100; i++) {
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
			mq.publishSyncMessage(topic, msg);
			cu.sleep(delayBetweenThings );
			created += 100;

			String url;

			Boolean repeat;
			Integer times;  //this is a patch, because the CoreBridge has an error

			for (i=0; i<100; i++) {
				repeat = true;
				times = 10;
				while (repeat) {
					try {
						url = "http://localhost:8080/riot-core-services/api/thing/" + castSerialNumber(baseSerial + 100 + i) + "/setParent/" + castSerialNumber(baseSerial + i);
						res = httpPostMessage(url, "");
						if (res.get("modifiedTime") != null) {
							repeat = false;
							created ++;
						} else {
							System.out.println("retry # " + (10 - times) + " en endpoint para serial: " + castSerialNumber(baseSerial + 100 + i));
							times--; //retry only 10 times, and then continue
							cu.sleep(1 + (10 - times) * delayBetweenThings/10);
							if (times <= 0) {
								repeat = false;
								errores ++;
								System.out.println("ERROR       en endpoint para serial: " + castSerialNumber(baseSerial + 100 + i));
								System.out.println("total created:" + created + "  errores:" + errores);
							}
						}
					} catch (Exception e) {
						System.out.println(e.getCause());
					}
				}

				repeat = true;
				times = 10;
				while (repeat) {
					try {
						url = "http://localhost:8080/riot-core-services/api/thing/" + castSerialNumber(baseSerial + 200 +i) + "/setParent/" + castSerialNumber(baseSerial + i);
						res = httpPostMessage(url, "");
						cu.sleep(1 + (10 - times) * delayBetweenThings/10);
						if (res.get("modifiedTime") != null) {
							repeat = false;
							created ++;
						} else {
							System.out.println("retry # " + (10 - times) + " en endpoint para serial: " + castSerialNumber(baseSerial + 200 + i));
							times--; //retry only 10 times, and then continue
							if (times <= 0) {
								repeat = false;
								errores ++;
								System.out.println("ERROR       en endpoint para serial: " + castSerialNumber(baseSerial + 200 + i));

								System.out.println("total created:" + created + "  errores:" + errores);
							}
						}
					} catch (Exception e) {
						System.out.println(e.getCause());
					}
				}
			}
		} catch (Exception e) {
			System.out.println(e.getCause());
		}
	}

	private void sendChangeMessage( Integer sequenceNumber, String serialNumber, String thingType, Integer delayBetweenThings)
	{

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
		mq.publishSyncMessage(topic, msg.toString());
		if(delayBetweenThings > 0)
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
		Long thingsToChange = 0L;
		Integer delayBetweenThings = 10;

		System.out.print(cu.black() + "\nHow many things wants to change?[" + cu.green() + "1000" + cu.black() + "]:");
		String tagIn = in.nextLine();
		if (tagIn.equals("")) {
			tagIn = "1000";
		} else {
			tagIn = "" + Long.parseLong(tagIn);
		}
		thingsToChange = Long.parseLong(tagIn);

		System.out.print(cu.black() + "\nHow many miliseconds (ms) between each blink ?[" + cu.green() + delayBetweenThings + cu.black() + "]:");
		tagIn = in.nextLine();
		if (tagIn.equals("")) {
			//delayBetweenThings = delayBetweenThings;
		} else {
			delayBetweenThings = Integer.parseInt( tagIn );
		}

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

		for (Integer i = 0; i < thingsToChange; ) {
			DBObject filterById = new BasicDBObject("_id", random.nextLong()%maxId);
			cursor = thingsCollection.find(filterById).limit(1);
			try {
				if (cursor.hasNext()) {
					cursor.next();
					serialNumber = cursor.curr().get("serialNumber").toString();
					thingType    = cursor.curr().get("thingTypeCode").toString();
					if ( allThings || thingType.equals( "forkliftBattery") || thingType.equals( "forkliftSolar") )
					{
						System.out.println( i + " " + serialNumber + " " + thingType );
						sendChangeMessage( i, serialNumber, thingType, delayBetweenThings );
						i ++;
					}
				}
			} finally {
				cursor.close();
			}

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

		sendChangeMessage( 1, serialNumber, thingType, 0 );

	}

	private void sendCommasBlink() {
		StringBuffer sb = new StringBuffer();

		Random r = new Random();
		String serialNumber = "";

		serialNumber = "000000000000000000000" + cu.prompt( "enter a serialNumber",lastSerialNumber );
		lastSerialNumber = serialNumber.substring(serialNumber.length()-21, serialNumber.length());
		serialNumber = lastSerialNumber;

		thingTypeCode = cu.prompt( "enter the thingTypeCode",thingTypeCode );

		thingField = cu.prompt( "enter the udf name",thingField );

		String fruits[] = {"apples", "oranges", "bananas", "grapes", "strawberries", "watermelon", "pineapples"};
		String commaValue = "";
		for (int i = 0; i < r.nextInt( 2 )+2; i++) {
			commaValue += (commaValue.equals( "" ) ? "" : ", " ) + (r.nextInt( 10 ) + 1) + " " + fruits[ r.nextInt( fruits.length -1)];
		}

		String topic = "/v1/data/ALEB/" + thingTypeCode;

		Long time = new Date().getTime();
		sb.append( " sn," + sequenceNumber + "\n" );
		sb.append(",0,___CS___,-118.443969;34.048092;0.0;20.0;ft\n");

		sb.append(serialNumber + "," + time + "," + thingField + "," + "\"" + commaValue + "\"\n");


		System.out.println(" serialNumber: " + cu.blue() + serialNumber + cu.black() + "");
		System.out.println("thingTypeCode: " + cu.blue() + thingTypeCode + cu.black() + "");
		System.out.println("        field: " + cu.blue() + thingField + cu.black() + "");
		System.out.println("        value: " + cu.blue() + commaValue + cu.black() + "");

		DBObject prevThing = cu.getThing( serialNumber );

		mq.publishSyncMessage(topic, sb.toString());
		cu.sleep( 1000 );

		DBObject newThing = cu.getThing( serialNumber );
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

		sb.append( " sn," + sequenceNumber + "\n" );

		Long time = new Date().getTime();
		sb.append( " sn," + sequenceNumber + "\n" );
		sb.append(",0,___CS___,-118.443969;34.048092;0.0;20.0;ft\n");

		sb.append(serialNumber + "," + time + "," + thingFieldJSON + "," + "\"" + jsonStr.toString() + "\"\n");


		System.out.println(" serialNumber: " + cu.blue() + serialNumber + cu.black() + "");
		System.out.println("thingTypeCode: " + cu.blue() + thingTypeCode + cu.black() + "");
		System.out.println("        field: " + cu.blue() + thingFieldJSON + cu.black() + "");
		System.out.println("        value: " + cu.blue() + jsonStr.toString() + cu.black() + "");

		DBObject prevThing = cu.getThing( serialNumber );

		mq.publishSyncMessage(topic, sb.toString());
		cu.sleep( 1000 );

		DBObject newThing = cu.getThing( serialNumber );
		cu.diffThings( newThing, prevThing );
	}



	public void incrementalMR()
	{
		DBCursor cursor;
		DBCollection mrlogsCollection = cu.db.getCollection("mrlogs");
		BasicDBList inList = new BasicDBList();
		BasicDBList inTypes = new BasicDBList();
		inTypes.add("forklift");
		inTypes.add("forkliftBattery");
		inTypes.add("forkliftSolar");
		try {
			DBObject filterById = new BasicDBObject("_id", new BasicDBObject("$in", inTypes));
			cursor = mrlogsCollection.find(filterById);
			try {
				while (cursor.hasNext()) {
					cursor.next();
					BasicDBList childrenList = (BasicDBList) cursor.curr().get("children");
					Iterator<Object> it = childrenList.iterator();
					while (it.hasNext()) {
						inList.add(it.next());
					}
					DBObject rmQuery = new BasicDBObject("_id", new BasicDBObject("$in", inTypes));
					mrlogsCollection.findAndRemove(rmQuery);
				}
			} finally {
				cursor.close();
			}

			System.out.println("there are " + inList.size() + " thing Ids in the log");

			TimerUtils tu = new TimerUtils();
			tu.mark();
			String childrenMap = read( "/childrenMap.txt" );
			String childrenReduce = read( "/childrenReduce.txt" );

			DBObject query = new BasicDBObject( "_id", new BasicDBObject("$in", inList));

			MapReduceCommand cmd = new MapReduceCommand(
					thingsCollection,
					childrenMap,
					childrenReduce,
					"mr_forklift",
					MapReduceCommand.OutputType.REDUCE,
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


	public void execute() {
		setup();
		HashMap<String, String> options = new LinkedHashMap<String,String>();

		options.put("1", "create Thing Types for Parent-Child test");
		options.put("2", "create 100k things for Parent-Child test");
		options.put("3", "execute MR for Parent-Children");
		options.put("4", "change 1000 things ");
		options.put("5", "change 1000 child things only");
		options.put("6", "change 1 things");
		options.put("7", "execute incremental MR");
		options.put("8", "send a CSV (comma separate values) to udf");
		options.put("9", "send a JSON to udf");
		//options.put("4", "delete things and thingtypes");

		Integer option = 0;
		while (option != null) {
			option = cu.showMenu("blink options", options );
			if (option != null) {
				if (option == 0) {
					createThingTypes();
				}
				if (option == 1) {
					createThings();
				}
				if (option == 2) {
					executeMR();
				}
				if (option == 3) {
					changeThings(true);
				}
				if (option == 4) {
					changeThings(false);
				}
				if (option == 5) {
					changeOneThing();
				}
				if (option == 6) {
					incrementalMR();
				}
				if (option == 7) {
					sendCommasBlink();
				}

				if (option == 8) {
					sendJSONBlink();
				}

				System.out.println(cu.black() +  "\npress [enter] to continue");
				Scanner in = new Scanner(System.in);
				in.nextLine();
			}
		}

	}

}
