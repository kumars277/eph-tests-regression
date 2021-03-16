package com.eph.automation.testing.configuration;

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
            case "SIT":
                secretName=getSITSecretName(connectionURL);

                /*switch (connectionURL) {
                    case "AWS_URL":     secretName = "eph_sit_aws_url";     break;
                    case "PROMIS_URL":  secretName = "eph_sit_promis_url";  break;
                    case "MYSQL_JM_URL":secretName = "eph_mysql_jm_sit";    break;
                    case "EPH_URL":     secretName = "eph_sit_url";         break;
                    default:throw new IllegalArgumentException("Illegal argument: " + connectionURL);
                }*/
                break;

            case "UAT":secretName=getUATSecretName(connectionURL);
                break;

            case "UAT2":secretName=getUAT2SecretName(connectionURL);
                break;

        }


        AWSSecretsManager client = AWSSecretsManagerClientBuilder.standard()
                .withRegion(region)
                .build();

        String secret = "";
        String decodedBinarySecret = "";

        GetSecretValueRequest getSecretValueRequest = new GetSecretValueRequest()
                .withSecretId(secretName);
        GetSecretValueResult getSecretValueResult = null;

        try {
            getSecretValueResult = client.getSecretValue(getSecretValueRequest);
        } catch (DecryptionFailureException | InternalServiceErrorException | InvalidParameterException | InvalidRequestException | ResourceNotFoundException e) {
            throw e;
        }

        if (getSecretValueResult.getSecretString() != null) {
            secret = getSecretValueResult.getSecretString();

        } else {
            decodedBinarySecret = new String(Base64.getDecoder().decode(getSecretValueResult.getSecretBinary()).array());
        }

        JSONParser parser = new JSONParser();
        try {
            object = (JSONObject) parser.parse(secret);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        String DBconnection = "";
        switch (connectionURL) {
            case "AWS_URL":
                return object.getAsString("SIT_AWS_URL");
            case "PROMIS_URL":
                return object.getAsString("SIT_PROMIS_URL");
            case "MYSQL_JM_URL":
                return object.getAsString("MYSQL_JM_URL");
            case "EPH_URL":
                DBconnection = DecryptionService.decrypt(object.getAsString("EPH_SIT_URL"));

        }

        return DBconnection;
    }

    public static String getSITSecretName(String connectionURL){
        String secretName="";
        switch (connectionURL) {
            case "AWS_URL":     secretName = "eph_sit_aws_url";     break;
            case "PROMIS_URL":  secretName = "eph_sit_promis_url";  break;
            case "MYSQL_JM_URL":secretName = "eph_mysql_jm_sit";    break;
            case "EPH_URL":     secretName = "eph_sit_url";         break;
            default:throw new IllegalArgumentException("Illegal argument: " + connectionURL);
        }
        return secretName;
    }

    public static String getUATSecretName(String connectionURL){
        String secretName="";
        switch (connectionURL) {
            case "AWS_URL":     secretName = "";  break;
            case "PROMIS_URL":  secretName = "";  break;
            case "MYSQL_JM_URL":secretName = "";  break;
            case "EPH_URL":     secretName = "";  break;
            default:throw new IllegalArgumentException("Illegal argument: " + connectionURL);
        }
        return secretName;
    }

    public static String getUAT2SecretName(String connectionURL){
        String secretName="";
        switch (connectionURL) {
            case "AWS_URL":     secretName = "";  break;
            case "PROMIS_URL":  secretName = "";  break;
            case "MYSQL_JM_URL":secretName = "";  break;
            case "EPH_URL":     secretName = "";  break;
            default:throw new IllegalArgumentException("Illegal argument: " + connectionURL);
        }
        return secretName;
    }

}
