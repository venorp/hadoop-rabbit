package org.apache.hadoop.rabbit.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.rabbit.configuration.RabbitConfiguration;

/**
 * ${RabbitConfiguration} instance factory
 */
public final class ConfigUtils {

	public static Configuration getConf(){
		return RabbitConfiguration.create();
	}
	
}
