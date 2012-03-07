package nl.tudelft.in4325;

import java.io.File;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ConfigurationHelper {

	private static final Log LOGGER = LogFactory.getLog(ConfigurationHelper.class);
	
	private Configuration appConfiguration = null;
	
	public Configuration getConfiguration(){
		if (appConfiguration == null){
			initConfiguration();
		}
		return appConfiguration;
	}
	
	/**
	 * Initiates application configuration from properties file
	 */
	public void initConfiguration(){
		String propertiesPath = Constants.DEFAULT_PROPERTIES_FILE;

		if (new File(Constants.PROPERTIES_FILE).exists()) {
			propertiesPath = Constants.PROPERTIES_FILE;
		}

		try {
			appConfiguration = new PropertiesConfiguration(propertiesPath);
		} catch (ConfigurationException e) {
			LOGGER.error("Application configuration was not initialized");
		}
	}

}
