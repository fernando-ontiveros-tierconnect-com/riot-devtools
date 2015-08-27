package com.tierconnect.utils;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;
import com.tierconnect.controllers.mapreduce;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;

/**
 * A common methods and utils for the data generator for the baku stadium example.
 *
 * @author feronti
 *
 */

public class CommonUtils
{
    final public String ANSI_RESET = "\u001B[0m";
    final public String ANSI_BLACK = "\u001B[30m";
    final public String ANSI_RED   = "\u001B[31m";
    final public String ANSI_GREEN = "\u001B[32m";
    final public String ANSI_BLUE  = "\u001B[34m";
    final public String ANSI_CLEAR = "\u001B[2J";
    final public String ANSI_EOF   = "\u001B[K";

    //properties used for mqtt connection
    public MqttClient mqttClient;
    MqttConnectOptions connOpts;
    MemoryPersistence persistence = new MemoryPersistence();

    //properties used for mongodb connection
    public MongoClient mongoClient;
    public DB db;
	DBCollection thingsCollection;
    int arrivedMessage = 0;

	//setup values
	String servicesApi;
	String mqtthost;
	String mqttport;


	public CommonUtils() {
    }

    public String moveTo(int x, int y)
    {
        String s = "\u001B[" + y + ";" + x +"H";
        return s;
    }

    public void setupMqtt(String broker, String clientId, int qos, String topic, MqttCallback callback)
    {
        //setup mqtt connection
        try {
            persistence = new MemoryPersistence();

            mqttClient = new MqttClient(broker, clientId, persistence);
            connOpts = new MqttConnectOptions();
            connOpts.setCleanSession(true);

            arrivedMessage = 0;
            //callback when the mqtt needs to receive messages
            if (callback != null) {
                mqttClient.setCallback(callback);
            }
            mqttClient.connect(connOpts);
			if (topic != null)
			{
				mqttClient.subscribe( topic );
			}
            System.out.println( ANSI_GREEN + "Connected to Mqtt broker: " + ANSI_BLACK + broker );

        } catch(MqttException me) {
            System.out.println(ANSI_RED);
            System.out.println(me.getMessage());
            System.out.println(me);
            System.out.println(ANSI_BLACK);
            me.printStackTrace();
            System.exit(0);
        }
    }

    public void setupMongodb(String mongoHost, int mongoPort, String mongoDatabase) {

        //setup Mongo connection
        try {
            mongoClient = new MongoClient( mongoHost, mongoPort );
            db = mongoClient.getDB( mongoDatabase );
            System.out.println(moveTo(1,10) + ANSI_GREEN + "Connected to " + mongoHost + ":" + mongoPort + "/" + mongoDatabase);
            System.out.print(moveTo(1,13) + ANSI_BLACK);

            mongoClient.setWriteConcern(WriteConcern.JOURNALED);
            //mongoClient.setWriteConcern(WriteConcern.UNACKNOWLEDGED);
			thingsCollection        = db.getCollection("things");

        } catch ( UnknownHostException me) {
            System.out.println(ANSI_RED);
            System.out.println(me.getMessage());
            System.out.println(me);
            System.out.println(ANSI_BLACK);
            me.printStackTrace();
            System.exit(0);
        } catch(MongoException me) {
            System.out.println(ANSI_RED);
            System.out.println(me.getMessage());
            System.out.println(me);
            System.out.println(ANSI_BLACK);
            me.printStackTrace();
            System.exit(0);
        }
    }

    public void setTitle( String title)
    {

        System.out.print (ANSI_CLEAR + ANSI_BLUE +  moveTo(0,0) );
        System.out.println(" __      ___ _______      ");
        System.out.println(" \\ \\    / (_)___  (_)     ");
        System.out.println("  \\ \\  / / _   / / ___  __");
        System.out.println("   \\ \\/ / | | / / | \\ \\/ /");
        System.out.println("    \\  /  | |/ /__| |>  < ");
        System.out.println("     \\/   |_/_____|_/_/\\_\\");
        System.out.println("  " + title);
        System.out.println(ANSI_BLACK );


        System.out.print ( moveTo(1,9) );
    }

    public Properties readConfigFile()
    {
        Properties  prop  = new Properties();
        InputStream input = null;

        try {
            String filename = System.getProperty("user.dir") + "/vizix.config";
            input = new FileInputStream(filename);

            prop.load(input);

			//generic values
			servicesApi = prop.getProperty( "services.api" );
			mqtthost    = prop.getProperty("mqtt.host");
			mqttport    = prop.getProperty("mqtt.port");

            return prop;

        } catch (IOException ex) {
            System.out.println(ANSI_RED);
            System.out.println(ex.getMessage());
            System.out.println("exception: "+ex);
            System.out.println(ANSI_BLACK);
            System.exit(0);
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            return prop;
        }
    }

	public String read( String fname ) throws IOException
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

	//menu functions
	public void showItemMenu(String i, String option)
	{
		System.out.print(ANSI_RED);
		System.out.print(i +". ");
		System.out.print(ANSI_BLACK);
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

	public Integer showMenu(String title, HashMap<String, String> options)
	{
		Iterator it = options.entrySet().iterator();

		Integer i = 0;
		Integer iOption = 0;

		System.out.println();
		System.out.println(ANSI_BLUE +  title);

		ArrayList<String> validOptions = new ArrayList<String>();
        for (String key: options.keySet()) {
			showItemMenu( key, options.get(key));
			validOptions.add(key);
		}

		showItemMenu("x", "back menu");
		validOptions.add("x");
		Scanner in = new Scanner(System.in);
		String option = "";

		while ( !isValidOption(option, validOptions) ) {
			System.out.print("select an option : ");
			option = in.nextLine();
		}

		String desc;
		if (option.equals("x")) {
			iOption = null;
			desc = "back menu";
		} else {
			iOption = Integer.parseInt(option) - 1;
			desc = options.get( option );
		}
		System.out.println("You selected : " + ANSI_GREEN + desc + ANSI_BLACK);
		return iOption;
	}

    public void sleep(Integer msecs) {
        try
        {
            Thread.sleep( msecs );
        }
        catch( InterruptedException e )
        {

        }
    }

    public void displayThing(DBObject doc) {
        if (doc == null) {
            System.out.println(ANSI_BLUE + "{\n}" + ANSI_BLACK);
            return;
        }
        System.out.println(ANSI_BLUE + "{" + ANSI_BLACK );
        Iterator<Map.Entry<String,Object>> it = ((BasicDBObject) doc).entrySet().iterator();

        //calculate max length
        Integer maxlength = 0;
        while (it.hasNext()) {
            Integer l = it.next().getKey().toString().length();
            if (l> maxlength) {
                maxlength = l;
            }
        }
        String maxpad = String.format("%"+maxlength+"s", " ");

        //second loop
        it = ((BasicDBObject) doc).entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String,Object> field;
            field = it.next();
			if (field.getValue()==null) {
				continue;
			}
            if (field.getValue().getClass().equals(String.class)) {
            }
            if (field.getValue().getClass().equals(BasicDBObject.class)) {
                String pad = maxpad+field.getKey();
                pad = pad.substring(pad.length()-maxlength, pad.length());
                System.out.println(ANSI_BLACK + pad + ANSI_BLACK + ": {" );
                Iterator<Map.Entry<String,Object>> itTwo = ((BasicDBObject) field.getValue() ).entrySet().iterator();
                while (itTwo.hasNext()) {
                    Map.Entry<String,Object> fieldTwo;
                    fieldTwo = itTwo.next();
                    //if (fieldTwo.getValue().getClass().equals(String.class)) {
                    System.out.println(maxpad + "  " + ANSI_BLACK + fieldTwo.getKey() + ANSI_BLACK + ": " + ANSI_BLUE + fieldTwo.getValue() + ANSI_BLACK);

                }
                System.out.println(ANSI_BLACK + String.format("%"+maxlength+"s", " ") + ANSI_BLACK + "}" );
            } else {
                String pad = String.format("%"+maxlength+"s", " ")+field.getKey();
                pad = pad.substring(pad.length()-maxlength, pad.length());
                System.out.println(ANSI_BLACK + pad + ANSI_BLACK + ": " + ANSI_BLUE + field.getValue() + ANSI_BLACK);
            }

        }
        System.out.println(ANSI_BLUE + "}" + ANSI_BLACK );
    }

	public void diffThings(DBObject newDoc, DBObject oldDoc) {

		if (newDoc == null && oldDoc != null) {
			displayThing(oldDoc);
			return;
		}
		if (newDoc == null && oldDoc == null) {
			System.out.println("thing does not exists!");
			return;
		}
		System.out.println(ANSI_BLUE + "{" + ANSI_BLACK );
		Iterator<Map.Entry<String,Object>> it = ((BasicDBObject) newDoc).entrySet().iterator();

		//calculate max length
		Integer maxlength = 0;
		while (it.hasNext()) {
			Integer l = it.next().getKey().toString().length();
			if (l> maxlength) {
				maxlength = l;
			}
		}
		String maxpad = String.format("%"+maxlength+"s", " ");

		//second loop
		it = ((BasicDBObject) newDoc).entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<String,Object> field;
			field = it.next();
            if (field.getValue() == null) {
				continue;
			}
			if (field.getValue().getClass().equals(BasicDBObject.class)) {
				String pad = maxpad+field.getKey();
				pad = pad.substring(pad.length()-maxlength, pad.length());
				System.out.println(ANSI_BLACK + pad + ANSI_BLACK + ": {" );
				Iterator<Map.Entry<String,Object>> itTwo = ((BasicDBObject) field.getValue() ).entrySet().iterator();
				while (itTwo.hasNext()) {
					Map.Entry<String,Object> fieldTwo;
					fieldTwo = itTwo.next();
					System.out.println(maxpad + "  " + ANSI_BLACK + fieldTwo.getKey() + ANSI_BLACK + ": " + ANSI_BLUE + fieldTwo.getValue() + ANSI_BLACK);

					if (oldDoc != null && (DBObject)oldDoc.get(field.getKey()) != null) {
						DBObject oldField = (DBObject) oldDoc.get(field.getKey());
						Object oldFieldTwo = oldField.get(fieldTwo.getKey());
						if (oldFieldTwo != null && !oldFieldTwo.equals(fieldTwo.getValue())) {
							pad = maxpad + "<old value>";
							pad = pad.substring(pad.length() - maxlength, pad.length());
							System.out.println(" " + ANSI_GREEN + pad + " " + ANSI_BLACK + fieldTwo.getKey() + ": " + ANSI_BLUE + oldFieldTwo + ANSI_BLACK);
						}
					}
				}
				System.out.println(ANSI_BLACK + String.format("%"+maxlength+"s", " ") + ANSI_BLACK + "}" );
			} else {
				String pad = String.format("%" + maxlength + "s", " ")+field.getKey();
				pad = pad.substring(pad.length()-maxlength, pad.length());
				System.out.println(ANSI_BLACK + pad + ANSI_BLACK + ": " + ANSI_BLUE + field.getValue() + ANSI_BLACK);
				if (oldDoc != null && oldDoc.get(field.getKey()) != null) {
					Object oldField = oldDoc.get(field.getKey());
					if (!oldField.equals(field.getValue())) {
						pad = String.format("%" + maxlength + "s", " ") + "<old value>";
						pad = pad.substring(pad.length() - maxlength, pad.length());
						System.out.println(ANSI_GREEN + pad + ANSI_BLACK + ": " + ANSI_BLUE + oldField + ANSI_BLACK);
					}
				}
			}
		}
		System.out.println(ANSI_BLUE + "}" + ANSI_BLACK );

	}

	public DBObject getThing( String serialNumber) {
        BasicDBObject query = new BasicDBObject("serialNumber", serialNumber);
        DBCursor cursor;
        DBObject doc = null;

        cursor = thingsCollection.find(query);

        try {
            while (cursor.hasNext()) {
                doc = cursor.next();
            }
        } finally {
            cursor.close();
        }

        return doc;
    }

	public String prompt(String title, String value)
	{
		StringBuffer sb = new StringBuffer();
		Scanner in;
		in = new Scanner( System.in );

		System.out.print( ANSI_BLACK + "\n" + title + "[" + ANSI_GREEN + value + ANSI_BLACK + "]:" );
		String tagIn = in.nextLine();
		if( tagIn.equals( "" ) )
		{
			tagIn = value;
		}
		return tagIn;
	}


	public HashMap<String,Object> httpGetMessage(String url) throws IOException, URISyntaxException
	{
		CloseableHttpClient httpclient = HttpClients.createDefault();

		HttpGet http = new HttpGet( servicesApi + url);
		HashMap<String,Object> res = new HashMap<String,Object>();

		http.setHeader("Content-Type", "application/json");
		http.setHeader("Api_key", "root");

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
				//System.out.println("Got " + res);
			} catch (Exception e) {

			}
		}
		finally
		{
			response.close();
			return res;
		}
	}

	private HashMap<String,Object> httpPutMessage(String url, String body) throws IOException, URISyntaxException
	{
		CloseableHttpClient httpclient = HttpClients.createDefault();

		HttpPut http = new HttpPut(servicesApi + url);
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

	public void publishSyncMessage(String topic, String content)
	{
		try {
			MqttMessage message = new MqttMessage(content.getBytes());
			mqttClient.publish(topic, message);
		}
		catch(MqttException me)
		{
			System.out.println("reason " + ANSI_RED + me.getReasonCode() + ANSI_BLACK);
			System.out.println("msg " + me.getMessage() );
		} catch(Exception e)
		{
			System.out.println("msg " + ANSI_RED + e.getMessage() + ANSI_BLACK);
		}
	}

	public void createThingTypeFromFile ( String thingTypeFile )
	{
		HashMap<String,Object> res;
		try {
			String body = read(thingTypeFile);

			res = httpPutMessage("http://localhost:8080/riot-core-services/api/thingType", body);

			if (res.containsKey( "error" ) )
			{
				System.out.println( ANSI_RED + res.get("error") + ANSI_BLACK);
			}
			else
			{
				System.out.println( res);
				//send a tickle to update corebridge
				System.out.println( ANSI_BLUE + "send a tickle to update corebridge" + ANSI_BLUE);
				sleep( 500 );

				String topic = "/v1/edge/dn/_ALL_/update/thingTypes";

				publishSyncMessage( topic, "" );
			}

		} catch (Exception e) {
			System.out.println(e.getCause());
		}

	}

}
