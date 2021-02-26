package com.eph.automation.testing.configuration;

import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.amazonaws.services.secretsmanager.model.*;
import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;
import net.minidev.json.parser.ParseException;

import java.util.Base64;


public class SecretsManagerHandler {
    static JSONObject object;

    public static Object getSecret (String connectionURL) throws ParseException {

        String secretName = "";
        String region = "eu-west-1";

        switch (connectionURL) {
            case "AWS_URL":
                secretName = "eph_sit_aws_url";
                break;
            case "PROMIS_URL":
                secretName = "eph_sit_promis_url";
                break;
            case "MYSQL_JM_URL":
                secretName = "eph_mysql_jm_sit";
                break;
            case "EPH_URL":
                secretName = "eph_sit_url";
                break;
            default:
                throw new IllegalArgumentException("Illegal argument: " + connectionURL);
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
        } catch (DecryptionFailureException|InternalServiceErrorException|InvalidParameterException|InvalidRequestException|ResourceNotFoundException e) {
            throw e;
        }

        if (getSecretValueResult.getSecretString() != null) {
            secret = getSecretValueResult.getSecretString();

        } else {
            decodedBinarySecret = new String(Base64.getDecoder().decode(getSecretValueResult.getSecretBinary()).array());
        }

        JSONParser parser = new JSONParser();
        object = (JSONObject) parser.parse(secret);


        switch (connectionURL) {
            case "AWS_URL":
                return object.getAsString("SIT_AWS_URL");
            case "PROMIS_URL":
               return object.getAsString("SIT_PROMIS_URL");
            case "MYSQL_JM_URL":
                return object.getAsString("MYSQL_JM_URL");
            case "EPH_URL":
                return object.getAsString("EPH_SIT_URL");

        }

        return "";
    }
}
