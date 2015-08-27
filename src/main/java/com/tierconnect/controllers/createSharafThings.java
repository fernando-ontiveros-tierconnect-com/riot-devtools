package com.tierconnect.controllers;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteOperation;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Scanner;
import java.util.Set;

/**
 * Created by fernando on 7/2/15.
 */
public class createSharafThings implements controllerInterface
{
	CommonUtils cu;

	Integer THING_PER_BLINK = 250;
	Long lastSerialNumber = 10000L;
	Long serialNumber     = 0L;
	Long lastQuantity = 10000L;
	Long sequenceNumber = 0L;

	Long thingsToChange = 0L;
	Integer delayBetweenThings = 100;
	Integer thingsPerBlink = 100;

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
		if ( isBetween( rand, 0.0, 0.7) ) {
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
				"Genious", "Garmin", "Fuji", "Ferrari", "Electrolux", "Dell", "Dlink", "Daewoo",
				"HP", "Casio", "Cannon", "Dell", "Atari", "IBM", "Yahoo", "Google"
		};
		Random r = new Random();

		int rand = r.nextInt( brands.length -1);


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

	public void setup()
	{
		thingsCollection        = cu.db.getCollection("things");
		thingsHistoryCollection = cu.db.getCollection("things_history");

		mq = new MqttUtils( "localhost", 1883);
	}


	public String getDescription() {
		return "Sharaf Enterprise";
	}

	public void createThingTypes() {
		cu.createThingTypeFromFile ("/sharafRFID.txt");

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
			StringBuffer msg = new StringBuffer(  );
			String topic, serial;

			Long time = new Date().getTime();
			Integer i;

			//the parent thing : forklift
			topic = "/v1/data/ALEB/sharafRFID";
			sequenceNumber = getSequenceNumber();
			msg.append( " sn," + sequenceNumber + "\n" );
			msg.append( ",0,___CS___,-118.443969;34.048092;0.0;20.0;ft\n");

			Date timeNow = new Date();
			Long now = timeNow.getTime();
			for (i=0; i < THING_PER_BLINK; i++) {
				serial = nextSerialNumber();

				//msg.append( serial + "," + time + ",sGroup," + "3" + "\n");
				//msg.append( serial + "," + time + ",facilityCode," + "3" + "\n");
				//msg.append( serial + "," + time + ",serialNUM," + serial + "\n");
				msg.append( serial + "," + time + ",lastLocateTime," + now + "\n");
				msg.append( serial + "," + time + ",lastDetectTime," + now + "\n");
				msg.append( serial + "," + time + ",Timestamp," + timeNow + "\n");
				msg.append( serial + "," + time + ",status," + getRandomStatus() + "\n");
				msg.append( serial + "," + time + ",itemcode," + getRandomItemCode() + "\n");
				msg.append( serial + "," + time + ",logicalReader," + getRandomLR() + "\n");
				msg.append( serial + "," + time + ",eNode," + getRandomEnode() + "\n");
				//msg.append( serial + "," + time + ",SKU," + "" + "\n");
				msg.append( serial + "," + time + ",IsAlreadyBuzzed," + getRandomBoolean() + "\n");
				msg.append( serial + "," + time + ",IsNotDetected," + getRandomBoolean() + "\n");
				msg.append( serial + "," + time + ",FindIT," + getRandomBoolean() + "\n");
				msg.append( serial + "," + time + ",IsMisplaced," + getRandomBoolean() + "\n");
				//msg.append( serial + "," + time + ",DocumentNum," + serialNumber + "\n");
				msg.append( serial + "," + time + ",Department,"  + "default department" + "\n");
				msg.append( serial + "," + time + ",supplier,"    + "default supplier for " + serial + "\n");
				msg.append( serial + "," + time + ",DisplayProduct," + "default display product for " + serial + "\n");
				msg.append( serial + "," + time + ",location,"    + "-118.443969;34.048092;0.0" + "\n");
				msg.append( serial + "," + time + ",locationXYZ," + "7.0;7.0;0.0" + "\n");
				msg.append( serial + "," + time + ",productDescrip," + "default product description for " + serial + "\n");
				//msg.append( serial + "," + time + ",image,"     + "default image for " + serial + "\n");
				msg.append( serial + "," + time + ",StockType," + "default stock type for " + serial + "\n");
				msg.append( serial + "," + time + ",brand,"     + getRandomBrand() + "\n");
				msg.append( serial + "," + time + ",AssignedZone," + getRandomZone() + "\n");
				//msg.append( serial + "," + time + ",registered,"   + getRandomBoolean() + "\n");
				msg.append( serial + "," + time + ",price,"   + getRandomPrice() + "\n");
				msg.append( serial + "," + time + ",touches," + "0" + "\n");
				msg.append( serial + "," + time + ",likes,"   + "0" + "\n");

			}

			mq.publishSyncMessage(topic, msg.toString());
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

        lastSerialNumber = Long.parseLong( cu.prompt( "enter the starting serialNumber", ""+lastSerialNumber  ));
		serialNumber = lastSerialNumber;


		//quantity
		lastQuantity = Long.parseLong( cu.prompt( "enter the quantity of things to create", ""+lastQuantity  ));
		Long quantity = lastQuantity;

		//things per blink
		THING_PER_BLINK = Integer.parseInt( cu.prompt( "enter the quantity of things per blink message", "" + THING_PER_BLINK ) );

		//delay between things
		delayBetweenThings = Integer.parseInt( cu.prompt( "enter the quantity of milliseconds betweeen blink", "" + delayBetweenThings ) );

		errores = 0L;
		created = 0L;
		TimerUtils tu = new TimerUtils();
		tu.mark();

		for (Long i=0L; i < quantity/THING_PER_BLINK; i++) {
			createHundredThings(delayBetweenThings);
			tu.mark();
			System.out.println("      created:" + created + "  time:" + tu.getLastDelt() + " ms.  sn:" + sequenceNumber);
		}
		System.out.println("TOTAL created:" + created + "  time:" + tu.getTotalDelt() + " ms.  sn:" + sequenceNumber);

	}

	private void sendChangeMessage( Long sequenceNumber, Set<String> serials, String thingType, Integer delayBetweenThings)
	{

		String topic = "/v1/data/ALEB/" + thingType;
		StringBuffer msg = new StringBuffer();
		msg.append(" sn," + sequenceNumber + "\n");
		msg.append(",0,___CS___,-118.443969;34.048092;0.0;20.0;ft\n");
		Long time = new Date().getTime();
		Random r = new Random();

		Iterator<String> it = serials.iterator();

		while ( it.hasNext())
		{
			String serialNumber = it.next();
			msg.append( serialNumber + "," + time + ",lastLocateTime," + time + "\n" );
			msg.append( serialNumber + "," + time + ",lastDetectTime," + time + "\n" );
			msg.append( serialNumber + "," + time + ",status," + getRandomStatus() + "\n" );
			msg.append( serialNumber + "," + time + ",locationXYZ," + r.nextInt( 499 ) + ".0;" + r.nextInt( 499 ) + ".0;0.0\n" );
			msg.append( serialNumber + "," + time + ",brand," + getRandomBrand() + "\n" );

			if( r.nextDouble() < 0.5 )
			{
				msg.append( serialNumber + "," + time + ",logicalReader," + getRandomLR() + "\n" );
				msg.append( serialNumber + "," + time + ",IsAlreadyBuzzed," + getRandomBoolean() + "\n" );
				msg.append( serialNumber + "," + time + ",IsNotDetected," + getRandomBoolean() + "\n" );
			}
			if( r.nextDouble() < 0.5 )
			{
				msg.append( serialNumber + "," + time + ",FindIT," + getRandomBoolean() + "\n" );
				msg.append( serialNumber + "," + time + ",IsMisplaced," + getRandomBoolean() + "\n" );
			}
			if( r.nextDouble() < 0.5 )
			{
				msg.append( serialNumber + "," + time + ",AssignedZone," + getRandomZone() + "\n" );
				msg.append( serialNumber + "," + time + ",touches," + r.nextInt( 499 ) + "\n" );
			}
			if( r.nextDouble() < 0.5 )
			{
				msg.append( serialNumber + "," + time + ",price," + getRandomPrice() + "\n" );
				msg.append( serialNumber + "," + time + ",likes," + r.nextInt( 499 ) + "\n" );
			}

		}

		mq.publishSyncMessage(topic, msg.toString());
		if (delayBetweenThings > 0)
		{
			cu.sleep( delayBetweenThings );
		}

	}



	private void changeThings(  )
	{
		Scanner in;
		in = new Scanner(System.in);

		thingsToChange = Long.parseLong( cu.prompt( "How many things wants to change?", ""+thingsToChange  ));

		delayBetweenThings = Integer.parseInt( cu.prompt( "How many miliseconds (ms) between each blink ?", "" + delayBetweenThings ) );

		thingsPerBlink = Integer.parseInt( cu.prompt( "How many things in each blink message?", "" + thingsPerBlink ) );

		System.out.print(cu.ANSI_BLACK + "\nChanging " + thingsToChange + " things with a delay of " + delayBetweenThings + " ms.");

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

		Set<String> serials = new HashSet<>( thingsPerBlink );
		for (Integer i = 0; i < thingsToChange; ) {
			DBObject filterById = new BasicDBObject("_id", random.nextLong()%maxId);
			cursor = thingsCollection.find(filterById).limit(1);
			try {
				if (cursor.hasNext()) {
					cursor.next();
					serialNumber = cursor.curr().get("serialNumber").toString();
					thingType    = cursor.curr().get("thingTypeCode").toString();
					if ( thingType.equals( "sharafRFID") && !serials.contains( serialNumber ) )
					{
						serials.add( serialNumber );
						i ++;
						if (i % thingsPerBlink == 0)
						{
							sequenceNumber = getSequenceNumber();
							sendChangeMessage( sequenceNumber, serials, thingType, delayBetweenThings );
							System.out.println( "sn:" + sequenceNumber + " " + i + " thing updates" );
							serials.clear();
						}
					}
				}
			} finally {
				cursor.close();
			}

		}


	}

	public Long getSequenceNumber()
	{
		Long sequenceNumber = 0L;

		DBCollection devtoolConfig = cu.db.getCollection( "devtoolsConfig" );

		BasicDBObject query = new BasicDBObject( "_id", "sequenceNumber" );
		BasicDBObject update = new BasicDBObject( "$inc", new BasicDBObject( "value", 1) );
		DBObject res = devtoolConfig.findAndModify( query, update );
		if (res == null) {
			devtoolConfig.insert( new BasicDBObject("_id", "sequenceNumber").append( "value", 0 ) );
			sequenceNumber = 1L;
		}
		else
		{
			sequenceNumber = Long.parseLong( res.get( "value" ).toString() );
		}

		return sequenceNumber;
	}

	public void execute() {
		setup();

		HashMap<String, String> options = new HashMap<String,String>();

		options.put("1", "create sharafRFI ThingType");
		options.put("2", "create Things for SharafRFID");
		options.put("3", "blink sharafRFID things");

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
					changeThings();
				}

				System.out.println(cu.ANSI_BLACK +  "\npress [enter] to continue");
				Scanner in = new Scanner(System.in);
				in.nextLine();
			}
		}

	}
}
