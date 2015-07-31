package com.tierconnect.controllers;

import com.mongodb.BasicDBObject;
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
import java.util.Random;
import java.util.Scanner;

/**
 * Created by fernando on 7/2/15.
 */
public class basicBlink  implements controllerInterface {

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
		return "send blinks to Ale Bridge";
	}

	public void setup()
	{
		thingsCollection        = cu.db.getCollection("things");
	}

	private void sendSimpleBlink(String tag, Boolean random) {
		StringBuffer sb = new StringBuffer();
		Scanner in;
		in = new Scanner(System.in);

		Random r = new Random();
		Integer posx = 0, posy = 0;
		String serialNumber = "";
		String thingTypeCode = "default_rfid_thingtype";
		String lr = "LR5";

		if (tag == null) {
			System.out.print(cu.ANSI_BLACK + "\nenter a serialNumber[" + cu.ANSI_GREEN + lastSerialNumber + cu.ANSI_BLACK + "]:");
			String tagIn = in.nextLine();
			if (tagIn.equals("")) {
				tagIn = lastSerialNumber;
			} else {
				tagIn = "000000000000000000000" + tagIn;
			}
			tag = tagIn.substring(tagIn.length()-21, tagIn.length());
			lastSerialNumber = tag;

		}

		if (random == null) {
			//posx
			System.out.print(cu.ANSI_BLACK + "\nenter locationX[" + cu.ANSI_GREEN + lastPosx + cu.ANSI_BLACK + "]:");
			String posxIn = in.nextLine();
			if (posxIn.equals("")) {
				posx = lastPosx;
			} else {
				posx = Integer.parseInt( posxIn);
			}
			lastPosx = posx;

			//posy
			System.out.print(cu.ANSI_BLACK + "\nenter locationY[" + cu.ANSI_GREEN + lastPosy + cu.ANSI_BLACK + "]:");
			String posyIn = in.nextLine();
			if (posyIn.equals("")) {
				posy = lastPosy;
			} else {
				posy = Integer.parseInt( posyIn );
			}
			lastPosy = posy;

		} else {
			if (random == true) {
				posx = r.nextInt(499);
				posy = r.nextInt(499);
				lr = "LR" + r.nextInt(10);
			}

			if (random == false) {
				posx = 37;
				posy = 44;
			}
		}

		sb.append("EPOCH,NOW\n");
		sb.append("DELT,REL\n");
		sb.append("\n");
		sb.append("CS,-118.443969,34.048092,0.0,20.0,ft\n");
		sb.append("LOC, 00:00:00," + tag + "," + posx + "," + posy + ",0," + lr + ",x3ed9371\n");

		System.out.println("serialNumber: " + cu.ANSI_BLUE + tag + cu.ANSI_BLACK + "");
		System.out.println("   locationX: " + cu.ANSI_BLUE + posx + cu.ANSI_BLACK + "");
		System.out.println("   locationY: " + cu.ANSI_BLUE + posy + cu.ANSI_BLACK + "");

		DBObject prevThing = cu.getThing(tag);

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
			System.out.println("AleXML generated a " + cu.ANSI_BLUE + output.toString().length() + cu.ANSI_BLACK + " bytes message");

			String[] args = {""};
			ALEPost alep = new ALEPost( args );
			alep.run( new ByteArrayInputStream(output.toString().getBytes()) );

			cu.sleep(1000);
			//cu.displayThing(prevThing);
			//cu.displayThing(cu.getThing(tag));
			cu.diffThings(cu.getThing(tag), prevThing);

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
		HashMap<String, String> options = new HashMap<String,String>();

		options.put("1", "send 000000000000000000100 with default position");
		options.put("2", "send 000000000000000000100 with random position");
		options.put("3", "send 000000000000000000100 and ask position");
		options.put("4", "send a tag and ask position");
		options.put("5", "send a tag with random position");
		options.put("6", "get Thing from Mongo");

		Integer option = 0;
		while (option != null) {
			option = cu.showMenu("blink options", options );
			if (option != null) {
				if (option == 0) {
					sendSimpleBlink("000000000000000000100", false);
				}
				if (option == 1) {
					sendSimpleBlink("000000000000000000100", true);
				}
				if (option == 2) {
					sendSimpleBlink("000000000000000000100", null);
				}
				if (option == 3) {
					sendSimpleBlink(null, null);
				}
				if (option == 4) {
					sendSimpleBlink(null, true);
				}
				if (option == 5) {
					getThingFromMongo();
				}

				System.out.println(cu.ANSI_BLACK +  "\npress [enter] to continue");
				Scanner in = new Scanner(System.in);
				in.nextLine();
			}
		}

	}

}
