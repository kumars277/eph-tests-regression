package com.eph.automation.testing.helper;

import java.util.Locale;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by RAVIVARMANS on 8/31/2017.
 */
public class CommonUtils {

    public static String getRandomName() {
        Random r = new Random();
        String alphabet = "abcdefghijklmnopqrstuvwxyz";
        final int N = 5;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < N; i++) {
            sb.append(alphabet.charAt(r.nextInt(alphabet.length())));
        }
        return sb.toString();
    }

    public static int getRandomNumForGivenRange(int min,int max) {
        return ThreadLocalRandom.current().nextInt(min, max + 1);
    }

    public static String getRandomProductWorkID() {
        String workIDPrefix = "EPR-W-";
        return workIDPrefix+getRandomCode();
    }

    public static String getRandomCountryISOCode() {
        String[] countries = Locale.getISOCountries();
        Locale locale = new Locale("en",countries[CommonUtils.getRandomNumForGivenRange(0,countries.length -1)]);
        return locale.getISO3Country();
    }

    public static String getRandomCode() {
        return String.valueOf((new Random()).nextInt(999) + 10000);
    }
}
