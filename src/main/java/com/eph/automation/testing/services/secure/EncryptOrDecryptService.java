package com.eph.automation.testing.services.secure;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.LoadProperties;
import org.jasypt.util.text.BasicTextEncryptor;

/**
 * Created by RAVIVARMANS on 5/10/2018.
 */
public class EncryptOrDecryptService {

    public static String decrypt(String textToDecrypt) {
        BasicTextEncryptor textDecryptor = new BasicTextEncryptor();
        textDecryptor.setPassword(LoadProperties.getProperty(Constants.ENCRYPTION_PASSWORD));
        return textDecryptor.decrypt(textToDecrypt);
    }

    public static String encrypt(String textToEncrypt) {
        BasicTextEncryptor textEncryptor = new BasicTextEncryptor();
        textEncryptor.setPassword(LoadProperties.getProperty(Constants.ENCRYPTION_PASSWORD));
        return textEncryptor.encrypt(textToEncrypt);
    }
}
