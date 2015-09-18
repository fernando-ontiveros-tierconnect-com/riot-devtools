package com.tierconnect.controllers;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.tierconnect.dev.controllerInterface;
import com.tierconnect.riot.bridges.ale.utils.ALEPost;
import com.tierconnect.riot.bridges.ale.utils.ALEXMLMessage;
import com.tierconnect.utils.CommonUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Random;
import java.util.Scanner;

/**
 * Created by fernando on 7/2/15.
 */
public class zonesBlink  implements controllerInterface {

	CommonUtils cu;
	String lastSerialNumber = "000000000000000000001";
	String thingTypeCode = "default_rfid_thingtype";

	DBCollection thingsCollection;

	public void setCu(CommonUtils cu) {
		this.cu = cu;
	}


	public String getDescription() {
		return "send Zone changes blinks to Ale Bridge";
	}

	public void setup()
	{
		thingsCollection        = cu.db.getCollection("things");
	}

	private void sendZoneBlink(String tag, String zone) {
		StringBuffer sb = new StringBuffer();
		Scanner in;
		in = new Scanner(System.in);

		Random r = new Random();
		Integer posx = 0, posy = 0;
		String serialNumber = "";
		String lr = "LR5";

		if (tag == null) {
			serialNumber = "000000000000000000000" + cu.prompt( "enter a serialNumber", lastSerialNumber );
			lastSerialNumber = serialNumber.substring( serialNumber.length() - 21, serialNumber.length() );
			tag = lastSerialNumber;

			thingTypeCode = cu.prompt( "enter the thingTypeCode", thingTypeCode );

		}

		if (zone == "Stockroom") {
			posx = 37;
			posy = 44;
		}
		if (zone == "Salesfloor") {
			posx = 59;
			posy = 25;
		}
		if (zone == "POS") {
			posx = 18;
			posy = 12;
		}
		if (zone == "Entrance") {
			posx = 7;
			posy = 7;
		}

		if (zone == "Unknown") {
			posx = 250;
			posy = 250;
		}

		sb.append("EPOCH,NOW\n");
		sb.append("DELT,REL\n");
		sb.append("\n");
		sb.append("CS,-118.443969,34.048092,0.0,20.0,ft\n");
		sb.append("LOC, 00:00:00," + tag + "," + posx + "," + posy + ",0," + lr + ",x3ed9371\n");

		System.out.println("serialNumber: " + cu.blue() + tag + cu.black() + "");
		System.out.println("   locationX: " + cu.blue() + posx + cu.black() + "");
		System.out.println("   locationY: " + cu.blue() + posy + cu.black() + "");

		DBObject prevThing = cu.getThing(tag, thingTypeCode);

		OutputStream output = new OutputStream()
		{
			private StringBuilder string = new StringBuilder();
			@Override
			public void write(int b) throws IOException {
				this.string.append((char) b );
			}

			public String toString(){
				return this.string.toString();
			}
		};

		ALEXMLMessage aleXmlMessage;
		try {
			aleXmlMessage = new ALEXMLMessage();
			aleXmlMessage.run( new ByteArrayInputStream(sb.toString().getBytes()), (OutputStream) output);
			//aleXmlMessage.run( System.in, System.out );
			System.out.println("AleXML generated a " + cu.blue() + output.toString().length() + cu.black() + " bytes message");

			String[] args = {"-h" , cu.aleHost, "-p", cu.alePort };
			ALEPost alep = new ALEPost( args );
			alep.run( new ByteArrayInputStream(output.toString().getBytes()) );

			cu.sleep(1000);
			cu.diffThings(cu.getThing(tag, thingTypeCode), prevThing);

		} catch (IOException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		} catch (org.apache.commons.cli.ParseException e) {
			e.printStackTrace();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}


	}

	public void execute() {
		setup();
		HashMap<String, String> options = new LinkedHashMap<String,String>();

		options.put("1", "send 000000000000000000100 to Stockroom");
		options.put("2", "send 000000000000000000100 to Salesfloor");
		options.put("3", "send 000000000000000000100 to POS");
		options.put("4", "send 000000000000000000100 to Entrance");
		options.put("5", "send 000000000000000000100 to Unknown");
		options.put("6", "send a tag to Stockroom");
		options.put("7", "send a tag to SalesFloor");
		options.put("8", "send a tag to POS");
		options.put("9", "send a tag to Entrance");
		options.put("10", "send a tag to Unknown");

		Integer option = 0;
		while (option != null) {
			option = cu.showMenu("Zone blink options", options );
			if (option != null) {
				if (option == 0) {
					sendZoneBlink("000000000000000000100", "Stockroom");
				}
				if (option == 1) {
					sendZoneBlink("000000000000000000100", "Salesfloor");
				}
				if (option == 2) {
					sendZoneBlink("000000000000000000100", "POS");
				}
				if (option == 3) {
					sendZoneBlink("000000000000000000100", "Entrance");
				}
				if (option == 4) {
					sendZoneBlink("000000000000000000100", "Unknown");
				}
				if (option == 5) {
					sendZoneBlink(null, "Stockroom");
				}
				if (option == 6) {
					sendZoneBlink(null, "Salesfloor");
				}
				if (option == 7) {
					sendZoneBlink(null, "POS");
				}
				if (option == 8) {
					sendZoneBlink(null, "Entrance");
				}
				if (option == 9) {
					sendZoneBlink(null, "Unknown");
				}

				System.out.println(cu.black() +  "\npress [enter] to continue");
				Scanner in = new Scanner(System.in);
				in.nextLine();
			}
		}

	}

}
