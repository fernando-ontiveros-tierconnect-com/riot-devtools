package com.tierconnect.controllers;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
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

			res = mapper.readValue(resp, typeRef);
			System.out.println("Got " + res);

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
			res = httpPutMessage("http://localhost:8080/riot-core-services/api/thingType", children1);

			String children2 = read("/forkliftChildren2.txt");
			children2 = children2.replaceAll("PARENT_ID", parentId.toString());
			res = httpPutMessage("http://localhost:8080/riot-core-services/api/thingType", children2);


		} catch (Exception e) {
			System.out.println(e.getCause());
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
		StringBuffer sb = new StringBuffer();
		Scanner in;
		in = new Scanner(System.in);

		System.out.print(cu.ANSI_BLACK + "\nenter the starting serialNumber[" + cu.ANSI_GREEN + lastSerialNumber + cu.ANSI_BLACK + "]:");
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
		System.out.print(cu.ANSI_BLACK + "\nenter the quantity of things to create[" + cu.ANSI_GREEN + lastQuantity + cu.ANSI_BLACK + "]:");
		tagIn = in.nextLine();
		if (tagIn.equals("")) {
			tagIn = lastQuantity;
		} else {
			tagIn = "0" + tagIn;
		}
		lastQuantity = tagIn;
		Long quantity = Long.parseLong( lastQuantity);

		errores = 0L;
		created = 0L;

		for (Long i=0L; i < quantity/10; i++) {
			//createOneThing();
			createTenThings();
		}
		System.out.println("TOTAL created:" + created + "  errores:" + errores);

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

	public void createTenThings()
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
			cu.sleep(1000 );
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
							cu.sleep(1 + (10 - times) * 100);
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
						cu.sleep(1 + (10 - times) * 100);
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

	public void execute() {
		setup();
		HashMap<String, String> options = new HashMap<String,String>();

		options.put("1", "create thing types for Parent-Child test");
		options.put("2", "create 100k things for Parent-Child test");
 		options.put("3", "execute MR for Parent-Children");
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
					//executeMR();
				}

				System.out.println(cu.ANSI_BLACK +  "\npress [enter] to continue");
				Scanner in = new Scanner(System.in);
				in.nextLine();
			}
		}

	}

}
