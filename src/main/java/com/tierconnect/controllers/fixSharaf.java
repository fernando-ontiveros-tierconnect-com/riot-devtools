package com.tierconnect.controllers;

/**
 * Created by fernando on 7/23/15.
 */

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.tierconnect.dev.controllerInterface;
import com.tierconnect.utils.CommonUtils;

import java.util.HashMap;
import java.util.Scanner;

/**
 * Created by fernando on 7/2/15.
 */
public class fixSharaf  implements controllerInterface {

	private void getThingFromMongo() {
		String tag;
		StringBuffer sb = new StringBuffer();
		Scanner in;
		in = new Scanner(System.in);

		System.out.print(cu.ANSI_BLACK + "\nenter a serialNumber[" + cu.ANSI_GREEN + lastSerialNumber + cu.ANSI_BLACK + "]:");
		String tagIn = in.nextLine();
		if (tagIn.equals("")) {
			tagIn = lastSerialNumber;
		} else {
			tagIn = "000000000000000000000" + tagIn;
		}
		tag = tagIn.substring(tagIn.length()-21, tagIn.length());
		lastSerialNumber = tag;

		System.out.println("serialNumber: " + cu.ANSI_BLUE + tag + cu.ANSI_BLACK + "");

		DBObject prevThing = cu.getThing(tag);

		cu.displayThing(prevThing);
	}

	CommonUtils cu;
	String lastSerialNumber = "000000000000000000001";
	Integer lastPosx = 0;
	Integer lastPosy = 0;

	DBCollection thingsCollection;
	BasicDBObject docs[];

	public void setCu(CommonUtils cu) {
		this.cu = cu;
	}


	public String getDescription() {
		return "fix an issue in Sharaf";
	}

	public void setup()
	{
		thingsCollection        = cu.db.getCollection("things");
	}

	private void getThingsWithIssue() {

		StringBuffer sb = new StringBuffer();

		String jsQuery = "typeof this.facilityCode != 'undefined' && typeof this.facilityCode.value != 'undefined' && this.facilityCode.value != this.groupCode && this.facilityCode.value == 'I01' ";
	    BasicDBObject query = new BasicDBObject("$where", jsQuery);
		DBCursor cursor = thingsCollection.find(query);
		int i = 0;
		try {
			while (cursor.hasNext()) {
				DBObject doc = cursor.next();
				if (!sb.toString().equals("")) {
					sb.append(",");
				}
				i++;
				if (i%5==0) {
					i = 0; sb.append("\n");
				}
				sb.append(" '");
				sb.append(doc.get("serialNumber"));
				sb.append("'");
			}
			System.out.println( "UPDATE apc_thing SET group_id = 3 WHERE serial in [ \n" + sb.toString() + "] ;" );

		} finally {
			cursor.close();
		}

	}

	private void fixThingsWithIssue() {
		String jsQuery = "typeof this.facilityCode != 'undefined' && typeof this.facilityCode.value != 'undefined' && this.facilityCode.value != this.groupCode && this.facilityCode.value == 'I01' ";
		BasicDBObject query = new BasicDBObject("$where", jsQuery);

		BasicDBObject setDoc = new BasicDBObject("x", "a").append("y", "b");  //("groupCode", "I01")
		BasicDBObject updateDoc = new BasicDBObject("$set", setDoc);

		thingsCollection.update(query, updateDoc);
		DBCursor cursor = thingsCollection.find(query);
		try {
			while (cursor.hasNext()) {
				System.out.println(cursor.next());
			}
		} finally {
			cursor.close();
		}


	}

	public void execute() {
		setup();
		HashMap<String, String> options = new HashMap<String,String>();

		options.put("1", "execute query to get Thing with the issue");
		options.put("2", "fix ");

		Integer option = 0;
		while (option != null) {
			option = cu.showMenu("blink options", options );
			if (option != null) {
				if (option == 0) {
					getThingsWithIssue();
				}
				if (option == 1) {
					fixThingsWithIssue();
				}

				System.out.println(cu.ANSI_BLACK +  "\npress [enter] to continue");
				Scanner in = new Scanner(System.in);
				in.nextLine();
			}
		}

	}

}
