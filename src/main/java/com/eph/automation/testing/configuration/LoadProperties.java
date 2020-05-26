package com.eph.automation.testing.configuration;

import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.services.security.DecryptionService;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by RAVIVARMANS on 11/24/2018.
 */
public class LoadProperties {

    protected static Properties definition;
    protected static String propertiesFile;



    private static void findPropertiesFile() {
        //below line need to enable when running in jenkins.
<<<<<<< HEAD
<<<<<<< HEAD
     // propertiesFile = System.getProperty("user.dir") + "/src/main/resources/" + (System.getProperty("ENV") == null || "".equals(System.getProperty("ENV")) ? "UAT" : System.getProperty("ENV")) + ".properties";
        propertiesFile = System.getProperty("user.dir") + "/src/main/resources/SIT.properties";
=======
=======
>>>>>>> 49827f78d6416a0addf296aaae875d7f575cea9c
      propertiesFile = System.getProperty("user.dir") + "/src/main/resources/" + (System.getProperty("ENV") == null || "".equals(System.getProperty("ENV")) ? "SIT" : System.getProperty("ENV")) + ".properties";
      //  propertiesFile = System.getProperty("user.dir") + "/src/main/resources/UAT.properties";
>>>>>>> 49827f78d6416a0addf296aaae875d7f575cea9c
        Log.info("Environment used for the testing: " + System.getProperty("ENV"));
        Log.info(("Properties.file: " + propertiesFile));
    }

    private static void initialise() {
        InputStream inStream;
        try {
            findPropertiesFile();
            if (definition == null) {
//                inStream = new FileInputStream(System.getProperty("user.dir") + "/src/main/resources/config.properties");
                inStream = new FileInputStream(propertiesFile);
                definition = new Properties();
                definition.load(inStream);
            }
        } catch (Exception exception) {
            exception.printStackTrace();
        }
    }

    public static String getDBConnection(String connectionURL) {
        initialise();
        return DecryptionService.decrypt(definition.getProperty(connectionURL));
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
