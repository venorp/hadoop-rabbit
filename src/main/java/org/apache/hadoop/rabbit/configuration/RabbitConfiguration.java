package org.apache.hadoop.rabbit.configuration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.rabbit.util.RabbitConstants;

public class RabbitConfiguration extends Configuration{
	
	static{
		// add default rabbit config,using the same with hadoop parameter format
		addDefaultResource(RabbitConstants.RABBIT_DEFAULT_CONFIG_FILE);
		addDefaultResource(RabbitConstants.RABBIT_OVERWRITE_DEFAULT_CONFIG_FILE);
	}
	
	/**
	 * Create ${RabbitConfiguration} instance
	 * @return Configuration instance
	 */
	public static Configuration create(){
		return new RabbitConfiguration();
	}
	
}
