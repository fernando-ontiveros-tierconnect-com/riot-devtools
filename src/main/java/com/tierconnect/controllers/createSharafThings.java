package com.tierconnect.controllers;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteOperation;
import com.mongodb.DBCollection;
import com.mongodb.MongoException;
import com.tierconnect.dev.controllerInterface;
import com.tierconnect.utils.CommonUtils;
import com.tierconnect.utils.MqttUtils;
import com.tierconnect.utils.TimerUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
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
 * Created by fernando on 7/2/15.
 */
public class createSharafThings implements controllerInterface
{
	CommonUtils cu;

	final Integer THING_PER_BLINK = 250;
	Long lastSerialNumber = 10000L;
	Long serialNumber     = 0L;
	String lastQuantity = "10000";
	Long sequenceNumber = 0L;

	Long errores;
	Long created;


	DBCollection thingsCollection;
	DBCollection thingsHistoryCollection;
	BasicDBObject docs[];
	BulkWriteOperation builder;
	Long loopcounter = 0L;

	MqttUtils mq;

	public void setCu(CommonUtils cu) {
		this.cu = cu;
	}

	public static boolean isBetween(double x, double lower, double upper) {
		return lower <= x && x <= upper;
	}


	private String getRandomStatus()
	{
		String statuses [] = {"GRN", "LTI", "Sold", "PointOfSale", "Stolen" };
		Random r = new Random();

		String status = "";

		double rand = r.nextDouble();
		if ( isBetween( rand, 0, 0.4) ) {
			status = statuses[0];
		}
		if ( isBetween( rand, 0.4, 0.7) ) {
			status = statuses[1];
		}
		if ( isBetween( rand, 0.7, 0.9) ) {
			status = statuses[2];
		}
		if ( isBetween( rand, 0.9, 0.95) ) {
			status = statuses[3];
		}
		if ( isBetween( rand, 0.95, 1) ) {
			status = statuses[4];
		}

		return status;
	}


	private String getRandomBrand()
	{
		String brands [] = {
				"Samsumg", "Nokia", "Apple", "Sony", "Mitsubishi", "Radiohead", "Linksys", "Logitech",
				"IBM", "Lenovo", "LG", "Lava", "Lafaeda", "Kinect", "Kenwood", "Kesington",
				"Kaspersky", "Karcher", "Jumbox", "Jobri", "JBL", "Iris", "Honeywell", "Hitachi",
				"Genious", "Garmin", "Fuji", "Ferrari", "Electrolux", "Dell", "Dlink", "Daewoo"
		};
		Random r = new Random();

		int rand = r.nextInt( 24);


		return brands[ rand];
	}

	private String getRandomZone()
	{
		String zones [] = {
				"Electronics", "Exit Back", "Exit Front", "Gaming", "Home Appliances",
				"HVS", "IT", "IT Accessories", "Personal Care", "Accessories",
				"Stock Room 1", "Stock Room 2", "Stock Room 3", "Telecom", "Theather",
				"Phones", "Apple", "TV", "Toys", "DVDs",
				"Music", "Cables", "Furniture", "unknown", "Radio"
		};
		Random r = new Random();

		int rand = r.nextInt( 25);


		return zones[ rand];
	}


	private BasicDBObject getNewSharafThing(int index) {

		String serial = String.format( "%021d", index );
		Date timeNow = new Date();
		Random r = new Random();

		BasicDBObject locationbson = new BasicDBObject("x",  r.nextDouble()*3+49 )
				.append("y", r.nextDouble()*3+40)
				.append("z", 0);

		BasicDBObject locationxyzbson = new BasicDBObject("x", r.nextDouble()*50+100)
				.append("y", r.nextDouble()*50+100)
				.append("z", 0);

		BasicDBObject fields = new BasicDBObject("eNode", null)
				.append("IsNotDetected",   null)
				.append("Timestamp",       timeNow)
				.append("itemcode",        serial)
				.append("IsMisplaced",     null)
				.append("productDescrip",  "product description for product " + serial)
				.append("status",          getRandomStatus() )
				.append("doorEvent",       null)
				.append("shift",           null)
				.append("AssignedZone",    null)
				.append("facilityCode",    "Sharaf")
				.append("Group",           null)
				.append("StockType",       null)
				.append("registered",      null)
				.append("lastDetectTime",  null)
				.append("DisplayProduct",  null)
				.append("Department",      null)
				.append("zone",            getRandomZone() )
				.append("serialNUM",       serial)
				.append("logicalReader",   "L3-S45-LR")
				.append("location",        locationbson)
				.append("locationXYZ",     locationxyzbson)
				.append("brand",           getRandomBrand() )
				.append("lastLocateTime",  null)
				.append("supplier",        getRandomBrand() )
				.append("IsAlreadyBuzzed", null)
				.append("SKU",             null)
				.append("image",           null);

		BasicDBObject doc = new BasicDBObject("name", serial)
				.append("serial",            serial)
				.append("createdByUser_id",  1 )
				.append("group_id",          3)
				.append("groupTypeFloor_id", null)
				.append("parent_id",         null)
				.append("thingType_id",      3)
				.append("fields",            fields);

		return doc;
	}

	public void setup()
	{
		thingsCollection        = cu.db.getCollection("things");
		thingsHistoryCollection = cu.db.getCollection("things_history");

		mq = new MqttUtils( "localhost", 1883);

	}

	public void loop(int index)
	{
		loopcounter ++;
		System.out.print(index);

		BasicDBObject doc = getNewSharafThing(index);

		try {
			thingsCollection.insert(doc);
			System.out.println(doc);
		} catch(MongoException me) {
			System.out.println(cu.ANSI_RED);
			System.out.println(me.getMessage());
			System.out.println(me);
			System.out.println(cu.ANSI_BLACK);
			me.printStackTrace();
			System.exit(0);
		}

	}


	public String getDescription() {
		return "Sharaf Enterprise";
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
			sb.append( line + "\n" );
		}
		br.close();
		return sb.toString();
	}


	public void createThingTypes() {
		HashMap<String,Object> res;
		try {
			String body = read("/sharafRFID.txt");
			res = httpPutMessage("http://localhost:8080/riot-core-services/api/thingType", body);
			System.out.println( res);


		} catch (Exception e) {
			System.out.println(e.getCause());
		}
	}

	private HashMap<String,Object> httpPutMessage(String url, String body) throws IOException, URISyntaxException
	{
		CloseableHttpClient httpclient = HttpClients.createDefault();

		//HttpPost httppost = new HttpPost( url );
		HttpPut http = new HttpPut(url);
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

	public String formatSerialNumber ( Long n )
	{
		String s;
		String t = "0000000000" +  n.toString();
		t = t.substring(t.length()-10, t.length());
		s = "AE1" + t;

		return s;
	}

	private String nextSerialNumber() {
		serialNumber++;
		return formatSerialNumber( serialNumber );
	}

	private String getRandomPrice()
	{
		Random rnd = new Random();
		Integer r = rnd.nextInt(100000);

		return "" + r / 100.00;
	}

	private String getRandomEnode()
	{
		Random rnd = new Random();
		Integer r = rnd.nextInt(1000);

		return "enode" + r;
	}

	private String getRandomItemCode()
	{
		Random rnd = new Random();
		Integer r1 = rnd.nextInt(100);
		Integer r2 = rnd.nextInt(10000);

		return r1 + "-" + r2;
	}

	private String getRandomLR()
	{
		Random rnd = new Random();
		Integer r = rnd.nextInt(1000);

		return "LR" + r;
	}

	private String getRandomBoolean()
	{
		Random rnd = new Random();
		Integer r = rnd.nextInt(100);
		if (r < 50)
		{
			return "1";
		}

		return "0";
	}


	public void createHundredThings(Integer delayBetweenThings)
	{
		HashMap<String,Object> res;

		try {
			String topic, msg, serial, lr;
			Integer posx, posy;
			Random r = new Random();

			Long time = new Date().getTime();
			Integer i;
			Long baseSerial;


			baseSerial = serialNumber +1;
			//the parent thing : forklift
			topic = "/v1/data/ALEB/sharafRFID";
			sequenceNumber++;
			msg = " sn," + sequenceNumber + "\n";
			msg += ",0,___CS___,-118.443969;34.048092;0.0;20.0;ft\n";

			Date timeNow = new Date();
			Long now = timeNow.getTime();
			for (i=0; i < THING_PER_BLINK; i++) {
				serial = nextSerialNumber();

				msg += serial + "," + time + ",sGroup," + "3" + "\n";
				msg += serial + "," + time + ",facilityCode," + "3" + "\n";
				msg += serial + "," + time + ",serialNUM," + serial + "\n";
				msg += serial + "," + time + ",lastLocateTime," + now + "\n";
				msg += serial + "," + time + ",lastDetectTime," + now + "\n";
				msg += serial + "," + time + ",Timestamp," + timeNow + "\n";
				msg += serial + "," + time + ",status," + getRandomStatus() + "\n";
				msg += serial + "," + time + ",itemcode," + getRandomItemCode() + "\n";
				msg += serial + "," + time + ",logicalReader," + getRandomLR() + "\n";
				msg += serial + "," + time + ",eNode," + getRandomEnode() + "\n";
				//msg += serial + "," + time + ",doorEvent," + getRandomBoolean() + "\n";
				msg += serial + "," + time + ",SKU," + "" + "\n";
				msg += serial + "," + time + ",IsAlreadyBuzzed," + getRandomBoolean() + "\n";
				msg += serial + "," + time + ",IsNotDetected," + getRandomBoolean() + "\n";
				msg += serial + "," + time + ",FindIT," + getRandomBoolean() + "\n";
				msg += serial + "," + time + ",IsMisplaced," + getRandomBoolean() + "\n";
				msg += serial + "," + time + ",DocumentNum," + serialNumber + "\n";
				msg += serial + "," + time + ",Department,"  + "default department" + "\n";
				msg += serial + "," + time + ",supplier,"    + "default supplier for " + serial + "\n";
				msg += serial + "," + time + ",DisplayProduct," + "default display product for " + serial + "\n";
				msg += serial + "," + time + ",location,"    + "-118.443969;34.048092;0.0" + "\n";
				msg += serial + "," + time + ",locationXYZ," + "7.0;7.0;0.0" + "\n";
				msg += serial + "," + time + ",productDescrip," + "default product description for " + serial + "\n";
				msg += serial + "," + time + ",image,"     + "default image for " + serial + "\n";
				msg += serial + "," + time + ",StockType," + "default stock type for " + serial + "\n";
				msg += serial + "," + time + ",brand,"     + getRandomBrand() + "\n";
				msg += serial + "," + time + ",AssignedZone," + getRandomZone() + "\n";
				msg += serial + "," + time + ",registered,"   + getRandomBoolean() + "\n";
				msg += serial + "," + time + ",price,"   + getRandomPrice() + "\n";
				msg += serial + "," + time + ",touches," + "0" + "\n";
				msg += serial + "," + time + ",likes,"   + "0" + "\n";

			}

			mq.publishSyncMessage(topic, msg);
			cu.sleep(delayBetweenThings );

			created += THING_PER_BLINK;
		} catch (Exception e) {
			System.out.println(e.getCause());
		}
	}



	public void createThings()
	{
		String tag;
		Integer delayBetweenThings = 1000;
		StringBuffer sb = new StringBuffer();
		Scanner in;
		in = new Scanner(System.in);

		System.out.print(cu.ANSI_BLACK + "\nenter the starting serialNumber[" + cu.ANSI_GREEN + lastSerialNumber + cu.ANSI_BLACK + "]:");
		String tagIn = in.nextLine();
		if (!tagIn.equals("")) {
			lastSerialNumber = Long.parseLong( tagIn );
		}

		//String serialNumber = formatSerialNumber( lastSerialNumber );
		serialNumber = lastSerialNumber;

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

		System.out.print(cu.ANSI_BLACK + "\nHow many miliseconds (ms) between each blink ?[" + cu.ANSI_GREEN + delayBetweenThings + cu.ANSI_BLACK + "]:");
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

		for (Long i=0L; i < quantity/THING_PER_BLINK; i++) {
			createHundredThings(delayBetweenThings);
			tu.mark();
			System.out.println("      created:" + created + "  errores:" + errores + " time:" + tu.getLastDelt() + " ms.");
		}
		System.out.println("TOTAL created:" + created + "  errores:" + errores + " time:" + tu.getTotalDelt() + " ms.");

	}


	public void execute() {
		setup();

		HashMap<String, String> options = new HashMap<String,String>();

		options.put("1", "create Thing Types for Sharaf Enterprise");
		options.put("2", "create 100k things for Parent-Child test");

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

				System.out.println(cu.ANSI_BLACK +  "\npress [enter] to continue");
				Scanner in = new Scanner(System.in);
				in.nextLine();
			}
		}

	}
}
