package nl.tudelft.in4325;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;

public class ConfigurationHelper {

	private static final Log LOGGER = LogFactory.getLog(ConfigurationHelper.class);
	private Configuration appConfiguration = null;

	public ConfigurationHelper(){
		initConfiguration();
	}
	
	public Configuration getConfiguration(){
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
    
    private String getPlatform(){
        return getConfiguration().getString("target-platform");
    }
    
    private String getPath(){
        return getConfiguration().getString(getPlatform() + "-" + "path-prefix"); 
    }
    
    public String getPathDependentString(String value){
        return getPath() + getConfiguration().getString(value);
    }
    
    public String getPlatformDependentString(String value){
        return getConfiguration().getString(getPlatform() + "-" + value); 
    }
    
    public String getString(String value){
        return getConfiguration().getString(value);
    } 
    
    public int getInt(String value){
        return getConfiguration().getInt(value);
    }
        
    
    

}
