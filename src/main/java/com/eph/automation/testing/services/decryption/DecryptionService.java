package com.eph.automation.testing.services.decryption;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.LoadProperties;
import org.jasypt.util.text.BasicTextEncryptor;


/**
 * Created by RAVIVARMANS on 11/24/2018.
 */
public class DecryptionService {

    public static String decrypt(String textToDecrypt) {
        BasicTextEncryptor textEncryptor = new BasicTextEncryptor();
        textEncryptor.setPassword(LoadProperties.getProperty(Constants.ENCRYPTION_PASSWORD));
        return textEncryptor.decrypt(textToDecrypt);
    }

    public static String encrypt(String textToEncrypt) {
        BasicTextEncryptor textEncryptor = new BasicTextEncryptor();
        textEncryptor.setPassword(LoadProperties.getProperty(Constants.ENCRYPTION_PASSWORD));
        return textEncryptor.encrypt(textToEncrypt);
    }    

}
