package com.eph.automation.testing.configuration;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by RAVIVARMANS on 11/24/2018.
 */
public class LoadProperties {

    protected static Properties definition;

    private static void initialise() {
        InputStream inStream;
        try {
            if (definition == null) {
                inStream = new FileInputStream(System.getProperty("user.dir") + "/src/main/resources/config.properties");
                definition = new Properties();
                definition.load(inStream);
            }
        } catch (Exception exception) {
            exception.printStackTrace();
        }
    }

    public static String getEndPoint(String connectionURL) {
        initialise();
        return definition.getProperty(connectionURL);
    }

    public static String getProperty(String key) {
        initialise();
        String value = definition.getProperty(key);
        if (value == null || "".equals(value)) {
            value = System.getProperty(key);
            if (value == null) {
                System.out.println("Missing value for key : " + key);
            }
        }
        return value;
    }
}
