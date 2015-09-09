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
import java.util.LinkedHashMap;
import java.util.Random;
import java.util.Scanner;

/**
 * Created by fernando on 7/2/15.
 */
public class basicBlink  implements controllerInterface {

	CommonUtils cu;
	String lastSerialNumber = "000000000000000000100";
	String thingTypeCode = "default_rfid_thingtype";
	Integer lastPosx = 0;
	Integer lastPosy = 0;

	DBCollection thingsCollection;
	BasicDBObject docs[];


	private void getThingFromMongo() {
		String serialNumber;

		serialNumber = "000000000000000000000" + cu.prompt( "enter a serialNumber",lastSerialNumber );
		lastSerialNumber = serialNumber.substring(serialNumber.length()-21, serialNumber.length());
		serialNumber = lastSerialNumber;

		thingTypeCode = cu.prompt( "enter the thingTypeCode",thingTypeCode );

		System.out.println("serialNumber: " + cu.blue() + serialNumber + cu.black() + "");
		System.out.println("   thingType: " + cu.blue() + thingTypeCode + cu.black() + "");

		DBObject prevThing = cu.getThing(serialNumber, thingTypeCode);

		cu.displayThing(prevThing);
	}

	public void setCu(CommonUtils cu) {
		this.cu = cu;
	}


	public String getDescription() {
		return "send simple blinks to Ale Bridge";
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
		String lr = "LR5";


		serialNumber = "000000000000000000000" + cu.prompt( "enter a serialNumber", lastSerialNumber );
		lastSerialNumber = serialNumber.substring( serialNumber.length() - 21, serialNumber.length() );
		serialNumber = lastSerialNumber;

		thingTypeCode = cu.prompt( "enter the thingTypeCode", thingTypeCode );

		if (random == null) {
			//posx
			System.out.print(cu.black() + "\nenter locationX[" + cu.green() + lastPosx + cu.black() + "]:");
			String posxIn = in.nextLine();
			if (posxIn.equals("")) {
				posx = lastPosx;
			} else {
				posx = Integer.parseInt( posxIn);
			}
			lastPosx = posx;

			//posy
			System.out.print(cu.black() + "\nenter locationY[" + cu.green() + lastPosy + cu.black() + "]:");
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
		sb.append("LOC, 00:00:00," + serialNumber + "," + posx + "," + posy + ",0," + lr + ",x3ed9371\n");

		System.out.println("serialNumber: " + cu.blue() + serialNumber + cu.black() + "");
		System.out.println("   locationX: " + cu.blue() + posx + cu.black() + "");
		System.out.println("   locationY: " + cu.blue() + posy + cu.black() + "");

		DBObject prevThing = cu.getThing(serialNumber, thingTypeCode);

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
			cu.diffThings(cu.getThing(serialNumber, thingTypeCode), prevThing);

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

				System.out.println(cu.black() +  "\npress [enter] to continue");
				Scanner in = new Scanner(System.in);
				in.nextLine();
			}
		}

	}

}
