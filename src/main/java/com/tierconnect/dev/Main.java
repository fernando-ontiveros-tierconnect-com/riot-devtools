package com.tierconnect.dev;

import com.tierconnect.utils.CommonUtils;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;
import java.util.Scanner;

/**
 * Created by fernando on 7/1/15.
 */
public class Main {
	private static final Logger logger = Logger.getLogger( Main.class );

	CommonUtils cu;
	ArrayList<controllerInterface> controllers = new ArrayList<controllerInterface>();

	//batch size
	int batch_row_size;

	//properties used for mongodb connection
	String mongoHost;
	String mongoDatabase;
	int    mongoPort;

	String mqttHost;

	//loop control
	int arrivedMessage = 0;
	int stop = 0;
	int loopcounter = 0;
	int saving = 0;

	String cmdClasses[] = {
			"deprecadted basicBlink",
			"zonesBlink",
			"nativeObjects",
			"SharafEnterprise",
			"parentChildren",
			"timeseries",
			"exitReport",
			"stats",
			"bugs",
			"unknown"
	};

	public Main() {
	}

	public static void main(String[] args) {
		new Main().run();
	}

	public void initDefaultValues()
	{
		Properties prop = cu.readConfigFile();

		//mongodb properties
		mongoHost     = prop.getProperty("mongodb.host");
		mongoDatabase = prop.getProperty("mongodb.database");
		mongoPort     = Integer.parseInt(prop.getProperty("mongodb.port"));

		mqttHost      = prop.getProperty("mqtt.broker");

	}

	public void setup()
	{
		cu.setupMongodb(mongoHost, mongoPort, mongoDatabase);
	}


	public void teardown()
	{
	}

	public void showItemMenu(String i, String option)
	{
		System.out.print(cu.red());
		System.out.print(i +". ");
		System.out.print(cu.black());
		System.out.print(option);
		System.out.println();
	}

	private Boolean isValidOption (String str, ArrayList<String> items) {
		boolean contains = false;
		for (String item : items) {
			if (str.equalsIgnoreCase(item)) {
				contains = true;
				break; // No need to look further.
			}
		}
		return contains;
	}

	public Integer showMenu()
	{
		Iterator<controllerInterface> it = controllers.iterator();

		Integer i = 0;
		Integer iOption = 0;
		ArrayList<String> options = new ArrayList<String>();

		System.out.println();
		System.out.println(cu.blue() +  "Main Menu");

		options.add("x");

        while (it.hasNext()) {
			i++;
			controllerInterface c = it.next();
			showItemMenu( ""+i, c.getDescription() );
			options.add("" + i);
		}
        showItemMenu("x", "exit");
		Scanner in = new Scanner(System.in);

		String option = "";
		while ( !isValidOption(option, options) ) {
			System.out.print("select an option : ");
			option = in.nextLine();
		}

		String desc;
		if (option.equals("x")) {
			iOption = null;
			desc = "exit";
		} else {
			iOption = Integer.parseInt(option) -1;
			desc = controllers.get( iOption ).getDescription();
		}
		System.out.println("You selected : " + cu.green() + desc + cu.black());
		return iOption;
	}

	public controllerInterface loadClass(String whichClass) {
		try {
			Class clazz = Class.forName(whichClass);
			controllerInterface instance = (controllerInterface) clazz.newInstance();
			instance.setCu (this.cu);
			return instance;
		} catch (ClassNotFoundException e) {
			//System.out.println(e.getCause());
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
		return null;
	}

	public void run()
	{
		cu = new CommonUtils();
		cu.setTitle("Dev Tools");

		initDefaultValues();
		setup();

		for (Integer i = 0; i< cmdClasses.length; i++ ) {
			controllerInterface cmd = loadClass("com.tierconnect.controllers." + cmdClasses[i]);
			if (cmd != null) {
				controllers.add(cmd);
			}
		}

		Integer option = 0;
		while (option != null) {
			cu.simpleStats();
			cu.simpleStatsByCollection();
			option = showMenu();
			if (option != null) {
				controllers.get(option).execute();
			}
		}

		teardown();
		cu.closeMqtt();
	}

}
