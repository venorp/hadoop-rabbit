package org.apache.hadoop.rabbit.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class RabbitUtils {
	
	private static final Configuration config = ConfigUtils.getConf();
	
	public static Path getHomeDir(){
		return new Path(config.get(RabbitConstants.RABBIT_HOME_DIR));
	}
	
	public static Path getTmpDir(){
		return new Path(getHomeDir(),config.get(RabbitConstants.RABBIT_TMP_DIR));
	}

}
