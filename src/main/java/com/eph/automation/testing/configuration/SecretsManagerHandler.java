package com.eph.automation.testing.configuration;
/*
* implemented by Nishant @ 15 Mar 2021
* */
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.amazonaws.services.secretsmanager.model.*;
import com.eph.automation.testing.models.TestContext;
import com.eph.automation.testing.services.security.DecryptionService;
import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;
import net.minidev.json.parser.ParseException;

import java.util.Base64;


public class SecretsManagerHandler {
    static JSONObject object;

    public static String getSecret(String connectionURL) {

        String secretName = "";
        String region = "eu-west-1";

        switch (TestContext.getValues().environment) {
            case "SIT":secretName=getSITSecretName(connectionURL);  break;
            case "UAT":secretName=getUATSecretName(connectionURL);  break;
            case "UAT2":secretName=getUAT2SecretName(connectionURL);break;
        }

        AWSSecretsManager client = AWSSecretsManagerClientBuilder.standard().withRegion(region).build();

        String secret = "";
        String decodedBinarySecret = "";

        GetSecretValueRequest getSecretValueRequest = new GetSecretValueRequest().withSecretId(secretName);
        GetSecretValueResult getSecretValueResult = null;

        try {getSecretValueResult = client.getSecretValue(getSecretValueRequest);}
        catch (DecryptionFailureException | InternalServiceErrorException | InvalidParameterException | InvalidRequestException | ResourceNotFoundException e) {throw e;}

        if (getSecretValueResult.getSecretString() != null) {secret = getSecretValueResult.getSecretString();}
        else {decodedBinarySecret = new String(Base64.getDecoder().decode(getSecretValueResult.getSecretBinary()).array());}

        JSONParser parser = new JSONParser();
        try {object = (JSONObject) parser.parse(secret);}
        catch (ParseException e) {e.printStackTrace();}

        String DBconnection = "";
        switch(TestContext.getValues().environment) {
            case "SIT": DBconnection = getSITdbConnection(connectionURL);break;
            case "UAT": DBconnection = getUATdbConnection(connectionURL);break;
            case "UAT2":DBconnection = getUAT2dbConnection(connectionURL);break;
            default:throw new IllegalArgumentException("Illegal argument: " + connectionURL);
        }

        return DBconnection;
    }

    public static String getSITSecretName(String connectionURL){
        //created by Nishant @ 15 Mar 2021
        switch (connectionURL) {
            case "AWS_URL":     return "eph_sit_aws_url";
            case "PROMIS_URL":  return "eph_sit_promis_url";
            case "MYSQL_JM_URL":return "eph_mysql_jm_sit";
            case "EPH_URL":     return "eph_sit_url";
            default:throw new IllegalArgumentException("Illegal argument: " + connectionURL);
        }
    }

    public static String getUATSecretName(String connectionURL){
        //created by Nishant @ 15 Mar 2021
        switch (connectionURL) {
            case "AWS_URL":     return "";
            case "PROMIS_URL":  return "";
            case "MYSQL_JM_URL":return "";
            case "EPH_URL":     return "";
            default:throw new IllegalArgumentException("Illegal argument: " + connectionURL);
        }
    }

    public static String getUAT2SecretName(String connectionURL){
        //created by Nishant @ 15 Mar 2021
        switch (connectionURL) {
            case "AWS_URL":     return "";
            case "PROMIS_URL":  return "";
            case "MYSQL_JM_URL":return "";
            case "EPH_URL":     return "";
            default:throw new IllegalArgumentException("Illegal argument: " + connectionURL);
        }
    }

    public static String getSITdbConnection(String connectionURL){
        //created by Nishant @ 17 Mar 2021
        switch (connectionURL) {
            case "AWS_URL":            return DecryptionService.decrypt(object.getAsString("SIT_AWS_URL"));
            case "PROMIS_URL":         return DecryptionService.decrypt(object.getAsString("SIT_PROMIS_URL"));
            case "MYSQL_JM_URL":       return DecryptionService.decrypt(object.getAsString("MYSQL_JM_URL"));
            case "EPH_URL":            return DecryptionService.decrypt(object.getAsString("EPH_SIT_URL"));
            default:throw new IllegalArgumentException("Illegal argument: " + connectionURL);
        }
    }

    public static String getUATdbConnection(String connectionURL){
        //created by Nishant @ 17 Mar 2021
        switch (connectionURL) {
            case "AWS_URL":            return DecryptionService.decrypt(object.getAsString(""));
            case "PROMIS_URL":         return DecryptionService.decrypt(object.getAsString(""));
            case "MYSQL_JM_URL":       return DecryptionService.decrypt(object.getAsString(""));
            case "EPH_URL":            return DecryptionService.decrypt(object.getAsString(""));
            default:throw new IllegalArgumentException("Illegal argument: " + connectionURL);
        }
    }

    public static String getUAT2dbConnection(String connectionURL){
        //created by Nishant @ 17 Mar 2021
        switch (connectionURL) {
            case "AWS_URL":            return DecryptionService.decrypt(object.getAsString(""));
            case "PROMIS_URL":         return DecryptionService.decrypt(object.getAsString(""));
            case "MYSQL_JM_URL":       return DecryptionService.decrypt(object.getAsString(""));
            case "EPH_URL":            return DecryptionService.decrypt(object.getAsString(""));
            default:throw new IllegalArgumentException("Illegal argument: " + connectionURL);
        }
    }
}
