package com.ranga.util;

import com.ranga.exception.ConfigurationException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static com.ranga.constants.HDFSConstants.*;

/*
 * Used to build the Configuration object
 * @author Ranga Reddy
 *  @version 1.0
 *  @since 31/3/2020
 */

public class ConfigUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(HDFSClient.class);
    private static Configuration configuration = null;

    private ConfigUtils() {

    }

    public static Configuration createConf(Map<String, String> properties) {
        LOGGER.debug("Building the Configuration object using params {}", properties);

        if (configuration == null) {

            if (!properties.containsKey(FS_DEFAULT_FS)) {
                throw new ConfigurationException(FS_DEFAULT_FS + " value doesn't exist");
            }

            configuration = new Configuration();
            configuration.set(FS_DEFAULT_FS, properties.get(FS_DEFAULT_FS));

            if (properties.containsKey(HDFS_CORESITE)) {
                configuration.addResource(new Path(properties.get(HDFS_CORESITE)));
            }

            if (properties.containsKey(HDFS_HDFSSITE)) {
                configuration.addResource(new Path(properties.get(HDFS_HDFSSITE)));
            }

            if (properties.containsKey(HDFS_FILE_SYSTEM_URI)) {
                configuration.addResource(new Path(properties.get(HDFS_FILE_SYSTEM_URI)));
            }

            configuration.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
            configuration.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

            System.setProperty("hadoop.home.dir", "/");
            System.setProperty("HADOOP_USER_NAME", "hdfs");

            LOGGER.info("Configuration object built successfully with the params {}", properties);

        }
        return configuration;
    }
}