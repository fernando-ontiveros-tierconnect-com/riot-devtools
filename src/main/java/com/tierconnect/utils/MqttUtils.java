package com.tierconnect.utils;

import org.apache.log4j.Logger;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;


/**
 * MqttConnector Class is for connecting synchronous wrapper
 * @author rchirinos
 * @date 20150422
 */
public class MqttUtils
{
	private static final Logger logger = Logger.getLogger( MqttUtils.class );

	private static final String CLIENT_ID = "MQTT_CLIENT_RIOT";
	private static final int QOS = 2;
	MemoryPersistence persistence;
	MqttClient mqttClient;

	private String broker;

	/*******************************************************
	 * @description Constructor
	 * @param host serverURI of the broker
	 * @param port port of the broker
	 *******************************************************/
	public MqttUtils(String host, int port)
	{
		this.broker = "tcp://" + host + ":" + port;
		persistence = new MemoryPersistence();
		mqttClient = null;
		try
		{
			mqttClient = new MqttClient(broker, CLIENT_ID, persistence);
			MqttConnectOptions connOpts = new MqttConnectOptions();
			connOpts.setCleanSession(true);
			logger.info( "Connecting to broker: "+broker );
			mqttClient.connect(connOpts);
			logger.info( "Connected" );
		}
		catch(MqttException me)
		{
			logger.info("reason " + me.getReasonCode());
			logger.info("msg " + me.getMessage());
			logger.info("loc " + me.getLocalizedMessage());
			logger.info("cause " + me.getCause());
			logger.info("excep " + me);
			me.printStackTrace();
		} catch(Exception e)
		{
			logger.info(e.getMessage());
		}

	}
	/*******************************************************
	 * @description Method for publishing a message through Sync Mqtt
	 * @param topic   : Topic of the message
	 * @param content : Content of the message
	 *******************************************************/
	public void publishSyncMessage(String topic, String content)
	{
		try {
			//logger.info( "Publishing: "+content);
			MqttMessage message = new MqttMessage(content.getBytes());
			message.setQos(QOS);
			mqttClient.publish(topic, message);
			//logger.info( "Message published");
		}
		catch(MqttException me)
		{
			logger.info("reason " + me.getReasonCode());
			logger.info("msg " + me.getMessage());
			logger.info("loc " + me.getLocalizedMessage());
			logger.info("cause " + me.getCause());
			logger.info("excep " + me);
			me.printStackTrace();
		} catch(Exception e)
		{
			logger.info(e.getMessage());
		}
	}

}
