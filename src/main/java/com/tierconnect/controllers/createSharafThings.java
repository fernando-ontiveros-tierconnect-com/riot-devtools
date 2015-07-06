package com.tierconnect.controllers;

import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteOperation;
import com.mongodb.DBCollection;
import com.mongodb.MongoException;
import com.tierconnect.dev.controllerInterface;
import com.tierconnect.utils.CommonUtils;

import java.util.Date;
import java.util.Random;

/**
 * Created by fernando on 7/2/15.
 */
public class createSharafThings implements controllerInterface
{
	CommonUtils cu;

	DBCollection thingsCollection;
	DBCollection thingsHistoryCollection;
	BasicDBObject docs[];
	BulkWriteOperation builder;
	Long loopcounter = 0L;

	public void setCu(CommonUtils cu) {
		this.cu = cu;
	}

	public static boolean isBetween(double x, double lower, double upper) {
		return lower <= x && x <= upper;
	}


	private String getStatus()
	{
		String statuses [] = {"GRN", "LTI", "Sold" };
		Random r = new Random();

		String status = "";

		double rand = r.nextDouble();
		if ( isBetween( rand, 0, 0.5) ) {
			status = statuses[0];
		}
		if ( isBetween( rand, 0.5, 0.9) ) {
			status = statuses[1];
		}
		if ( isBetween( rand, 0.9, 1) ) {
			status = statuses[2];
		}

		return status;
	}


	private String getBrand()
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

	private String getZone()
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
				.append("status",          getStatus() )
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
				.append("zone",            getZone() )
				.append("serialNUM",       serial)
				.append("logicalReader",   "L3-S45-LR")
				.append("location",        locationbson)
				.append("locationXYZ",     locationxyzbson)
				.append("brand",           getBrand() )
				.append("lastLocateTime",  null)
				.append("supplier",        getBrand() )
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
		return "creates thousands of things for Sharaf";
	}

	public void execute() {
		setup();

		System.out.println("inserting 100 new things in sharaf");
		System.out.println( thingsCollection);

		for (int i = 0; i < 100; i++) {
			loop(i);
		}
	}
}
