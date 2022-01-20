package com.eph.automation.testing.configuration;
/*
* implemented by Nishant @ 15 Mar 2021
* */
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.xspec.L;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.amazonaws.services.secretsmanager.model.*;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.TestContext;
import com.eph.automation.testing.services.security.DecryptionService;
import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;
import net.minidev.json.parser.ParseException;

import java.util.Base64;


public class SecretsManagerHandler {

    public static String getDBConnection(String connectionURL) {
        String secretName = "";
        String DBconnection = "";
        Log.info("Test Environment... "+TestContext.getValues().environment);
        switch (TestContext.getValues().environment) {

            case "SIT": secretName=getSITSecretName(connectionURL);
                        DBconnection = getSITdbConnection(getSecretKeyObj("eu-west-1",secretName),connectionURL);break;

            case "UAT": secretName=getUATSecretName(connectionURL);Log.info("secrete name => "+secretName);
                        DBconnection = getUATdbConnection(getSecretKeyObj("eu-west-1",secretName),connectionURL);
                        Log.info("DBConection value = > "+DBconnection.toString());
                        break;

            case "UAT2":secretName=getUAT2SecretName(connectionURL);
                        DBconnection = getUAT2dbConnection(getSecretKeyObj("eu-west-1",secretName),connectionURL);break;

            default: throw new IllegalArgumentException("Illegal argument: " +TestContext.getValues().environment);
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
            default:throw new IllegalArgumentException("Illegal argument: " + connectionURL);}
    }

    public static String getUATSecretName(String connectionURL){
        //created by Nishant @ 15 Mar 2021
        switch (connectionURL) {
            case "AWS_URL":     return "eph_aws_uat_url";
            case "EPH_URL":     return "eph_postgre_uat_url";
            default:throw new IllegalArgumentException("Illegal argument: " + connectionURL);}
    }

    public static String getUAT2SecretName(String connectionURL){
        //created by Nishant @ 15 Mar 2021
        switch (connectionURL) {
            case "AWS_URL":     return "eph_aws_uat_url";
            case "EPH_URL":     return "eph_postgre_uat2_url";
            default:throw new IllegalArgumentException("Illegal argument: " + connectionURL);}
    }

    public static String getSITdbConnection(JSONObject object, String connectionURL){
        //created by Nishant @ 17 Mar 2021     //updated by Nishant @ 30 Dec 2021
        String connectionString = "";
        switch (connectionURL) {
           case "AWS_URL":            return object.getAsString("SIT_AWS_URL");//Jenkins profile
        //  Local profile
        //              return=  "jdbc:awsathena://AwsRegion=eu-west-1;s3OutputLocation=s3://com-elsevier-eph-masterdata-nonprod/sit;" +
        //               "AwsCredentialsProviderClass=com.simba.athena.amazonaws.auth.profile.ProfileCredentialsProvider;AwsCredentialsProviderArguments=default;";
            case "PROMIS_URL":         return DecryptionService.decrypt(object.getAsString("SIT_PROMIS_URL"));
            case "MYSQL_JM_URL":       return DecryptionService.decrypt(object.getAsString("MYSQL_JM_URL"));
            case "EPH_URL":            return DecryptionService.decrypt(object.getAsString("EPH_POSTGRE_SIT_URL"));
            default:throw new IllegalArgumentException("Illegal argument: " + connectionURL);
        }
    }

    public static String getUATdbConnection(JSONObject object,String connectionURL){
        //created by Nishant @ 17 Mar 2021
        String connectionString = "";
        switch (connectionURL) {
            case "AWS_URL":
                Log.info("UAT_AWS_URL value=> "+object.getAsString("UAT_AWS_URL"));
            return object.getAsString("UAT_AWS_URL");//Jenkins profile
        //   Local profile
        //               return=  "jdbc:awsathena://AwsRegion=eu-west-1;s3OutputLocation=s3://com-elsevier-eph-masterdata-uat/uat/logs;" +
        //                 "AwsCredentialsProviderClass=com.simba.athena.amazonaws.auth.profile.ProfileCredentialsProvider;AwsCredentialsProviderArguments=default;";

//                 Jenkins profile
       //                 connectionString=  "jdbc:awsathena://AwsRegion=eu-west-1;s3OutputLocation=s3://com-elsevier-eph-masterdata-uat/uat/logs;" +
       //                 "AwsCredentialsProviderClass=com.simba.athena.amazonaws.auth.InstanceProfileCredentialsProvider;";
        //        return connectionString;
            case "PROMIS_URL":         return DecryptionService.decrypt(object.getAsString(""));
            case "MYSQL_JM_URL":       return DecryptionService.decrypt(object.getAsString(""));
            case "EPH_URL":            return DecryptionService.decrypt(object.getAsString("EPH_POSTGRE_UAT_URL"));
            default:throw new IllegalArgumentException("Illegal argument: " + connectionURL);
        }
    }
    public static String getUAT2dbConnection(JSONObject object,String connectionURL){
        //created by Nishant @ 17 Mar 2021
        String connectionString = "";
        switch (connectionURL) {
            case "AWS_URL":
//                Jenkins profile
//                connectionString=  "jdbc:awsathena://AwsRegion=eu-west-1;s3OutputLocation=s3://com-elsevier-eph-masterdata-uat/uat/logs;" +
//                        "AwsCredentialsProviderClass=com.simba.athena.amazonaws.auth.InstanceProfileCredentialsProvider;";
//                return connectionString;
                return DecryptionService.decrypt(object.getAsString("UAT_AWS_URL"));
            case "PROMIS_URL":         return DecryptionService.decrypt(object.getAsString(""));
            case "MYSQL_JM_URL":       return DecryptionService.decrypt(object.getAsString(""));
            case "EPH_URL":            return DecryptionService.decrypt(object.getAsString("EPH_POSTGRE_UAT2_URL"));
            default:throw new IllegalArgumentException("Illegal argument: " + connectionURL);
        }
    }

    public static JSONObject getSecretKeyObj(String region, String secretName){
        //created by Nishant @  29 Mar 2021
        JSONObject object = null;

        AWSSecretsManager client = AWSSecretsManagerClientBuilder.standard()
                .withRegion(region)
                .build();

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
        return object;
    }

}
