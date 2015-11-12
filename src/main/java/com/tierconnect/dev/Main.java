package com.tierconnect.dev;

import com.google.common.collect.Lists;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import com.tierconnect.utils.CommonUtils;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.event.Event;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

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

		if (1 == 1 ) {return; }
		/** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);
		BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<Event>(1000);
		BlockingQueue<String> queue = new LinkedBlockingQueue<String>(10000);

		/** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
		Hosts hosebirdHosts = new HttpHosts( Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

		// Optional: set up some followings and track terms
		//List<Long> followings = Lists.newArrayList(1234L, 566788L);
		List<String> terms = Lists.newArrayList( "bolivia", "iot" );
		//hosebirdEndpoint.followings(followings);
		hosebirdEndpoint.trackTerms(terms);

		// These secrets should be read from a config file
		Authentication hosebirdAuth = new OAuth1("VA57m8lj4rSxAfX7ZeK5dT308", "ABYvNAccTyPJIW28wgKLGgPmOTO5vA7DEgVmUFeAH3luNxctU4", "340388072-Cjptp7KTFk12lH2Gry9qUpHGxJIOylOrPT6oMcgK", "wrF1H9rQbQjh9MlWzlWDCur6JVsMeAP4EApyJXOiBYSTL");

		// Build a hosebird client
		ClientBuilder builder = new ClientBuilder()
				.hosts(Constants.STREAM_HOST)
				.authentication(hosebirdAuth)
				.endpoint( hosebirdEndpoint )
				.processor( new StringDelimitedProcessor( queue ) )
				.eventMessageQueue( eventQueue );
		Client hosebirdClient = builder.build();

		try
		{
			hosebirdClient.connect();
			while( !hosebirdClient.isDone() )
			{
				String message = queue.take();

				DBObject record = (DBObject) JSON.parse( message);
				System.out.print( record.get( "lang" ) ); // print the message}
				System.out.print( "-" );
				System.out.print( ((BasicDBObject) record.get( "user" )).get( "screen_name" ) ); // print the message}
				System.out.print( "  " );
				System.out.println( record.get("text") ); // print the message}

				DBCollection twitterCollection = cu.db.getCollection( "twitter" );
				twitterCollection.insert( record);

			}
		} catch( Exception e ) {
			e.printStackTrace();
		}
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
