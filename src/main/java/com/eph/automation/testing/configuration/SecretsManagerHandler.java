package com.eph.automation.testing.configuration;
/*
 * implemented by Nishant @ 15 Mar 2021
 * */
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

import static java.lang.System.*;


public class SecretsManagerHandler {
    public SecretsManagerHandler(){//
    }
    private static String illegalArgument = "Illegal argument: ";
    private static String region = "eu-west-1";

    public static String getDBConnection(String connectionURL) throws NullPointerException {
        String secretName = "";
        String dbConnection = "";
        Log.info("Debug mode----"+TestContext.getValues().environment);
        switch (TestContext.getValues().environment) {
            case "SIT": secretName=getSITSecretName(connectionURL);
                dbConnection = getSITdbConnection(getSecretKeyObj(region,secretName),connectionURL);break;

            case "UAT": secretName=getUATSecretName(connectionURL);
                dbConnection = getUATdbConnection(getSecretKeyObj(region,secretName),connectionURL);
                Log.info("Debug mode----"+dbConnection);
                break;

            case "UAT2":secretName=getUAT2SecretName(connectionURL);
                dbConnection = getUAT2dbConnection(getSecretKeyObj(region,secretName),connectionURL);break;

            default: throw new IllegalArgumentException(illegalArgument +TestContext.getValues().environment);
        }
        return dbConnection;
    }

    public static String getSITSecretName(String connectionURL){
        //created by Nishant @ 15 Mar 2021
        switch (connectionURL) {
            case "AWS_URL":     return "eph_sit_aws_url";
            case "PROMIS_URL":  return "eph_sit_promis_url";
            case "MYSQL_JM_URL":return "eph_mysql_jm_sit";
            case "EPH_URL":     return "eph_sit_url";
            case "EPH_RP_URL":  return "eph_sit_rp_url";
            default:throw new IllegalArgumentException(illegalArgument+ connectionURL);}
    }

    public static String getUATSecretName(String connectionURL){
        //created by Nishant @ 15 Mar 2021
        switch (connectionURL) {
            case "AWS_URL":     return "eph_aws_uat_url";
            case "EPH_URL":     return "eph_postgre_uat_url";
            case "EPH_RP_URL":  return "eph_uat_rp_url";
            default:throw new IllegalArgumentException(illegalArgument + connectionURL);}
    }

    public static String getUAT2SecretName(String connectionURL){
        //created by Nishant @ 15 Mar 2021
        switch (connectionURL) {
            case "AWS_URL":     return "eph_aws_uat_url";
            case "EPH_URL":     return "eph_postgre_uat2_url";
            default:throw new IllegalArgumentException(illegalArgument+ connectionURL);}
    }

    public static String getSITdbConnection(JSONObject object, String connectionURL){
        //created by Nishant @ 17 Mar 2021     //updated by Nishant @ 30 Dec 2021
        switch (connectionURL) {
            case "AWS_URL":
                if(true) {//jenkins profile
                    return object.getAsString("SIT_AWS_URL");}
                else { //  Local profile
                    return "jdbc:awsathena://AwsRegion=eu-west-1;s3OutputLocation=s3://com-elsevier-eph-masterdata-nonprod/sit;"
                            + "AwsCredentialsProviderClass=com.simba.athena.amazonaws.auth.profile.ProfileCredentialsProvider;AwsCredentialsProviderArguments=default;";
                }
            case "PROMIS_URL":         return DecryptionService.decrypt(object.getAsString("SIT_PROMIS_URL"));
            case "MYSQL_JM_URL":       return DecryptionService.decrypt(object.getAsString("MYSQL_JM_URL"));
            case "EPH_URL":            return DecryptionService.decrypt(object.getAsString("EPH_POSTGRE_SIT_URL"));
            case "EPH_RP_URL":         return DecryptionService.decrypt(object.getAsString("EPH_RP_SIT_URL"));

            default:throw new IllegalArgumentException(illegalArgument + connectionURL);
        }
    }

    public static String getUATdbConnection(JSONObject object,String connectionURL){
        //created by Nishant @ 17 Mar 2021
        Log.info("Debug mode----"+connectionURL);
        switch (connectionURL) {
            case "AWS_URL":
                if(true) {//jenkins profile
                    return object.getAsString("UAT_AWS_URL");}
                else { //  Local profile
                    return "jdbc:awsathena://AwsRegion=eu-west-1;s3OutputLocation=s3://com-elsevier-eph-masterdata-uat/uat/logs;" +
                            "AwsCredentialsProviderClass=com.simba.athena.amazonaws.auth.profile.ProfileCredentialsProvider;AwsCredentialsProviderArguments=default;";
                }
            case "PROMIS_URL":         return DecryptionService.decrypt(object.getAsString(""));
            case "MYSQL_JM_URL":       return DecryptionService.decrypt(object.getAsString(""));
            case "EPH_URL":            return DecryptionService.decrypt(object.getAsString("EPH_POSTGRE_UAT_URL"));
            case "EPH_RP_URL":
                Log.info("Debug mode----"+connectionURL);
                return DecryptionService.decrypt(object.getAsString("EPH_RP_UAT_URL"));
            default:throw new IllegalArgumentException(illegalArgument + connectionURL);
        }
    }

    public static String getUAT2dbConnection(JSONObject object,String connectionURL){
        //created by Nishant @ 17 Mar 2021
        switch (connectionURL) {
            case "AWS_URL":return DecryptionService.decrypt(object.getAsString("UAT_AWS_URL"));
//           Jenkins profile
//           return "jdbc:awsathena://AwsRegion=eu-west-1;s3OutputLocation=s3://com-elsevier-eph-masterdata-uat/uat/logs;" +
//           "AwsCredentialsProviderClass=com.simba.athena.amazonaws.auth.InstanceProfileCredentialsProvider;";
            case "PROMIS_URL":         return DecryptionService.decrypt(object.getAsString(""));
            case "MYSQL_JM_URL":       return DecryptionService.decrypt(object.getAsString(""));
            case "EPH_URL":            return DecryptionService.decrypt(object.getAsString("EPH_POSTGRE_UAT2_URL"));
            case "EPH_RP_URL":         return DecryptionService.decrypt(object.getAsString("EPH_RP_UAT_URL"));
            default:throw new IllegalArgumentException(illegalArgument + connectionURL);
        }
    }

    public static JSONObject getSMKeys(String secretName)
    {
        //by Nishant @12 Apr 2022 to access secrete manager keys
      //  String secretName = "eph_svcUsers"; //svc4/svc4pwd
        JSONObject object = getSecretKeyObj(region,secretName);
        return object;
    }

    public static JSONObject getSecretKeyObj(String region, String secretName) throws DecryptionFailureException, InternalServiceErrorException, InvalidParameterException, InvalidRequestException, ResourceNotFoundException {
        //created by Nishant @  29 Mar 2021
        JSONObject object = null;
        String secret = "";

        AWSSecretsManager client = AWSSecretsManagerClientBuilder.standard()
                .withRegion(region).build();

        GetSecretValueRequest getSecretValueRequest = new GetSecretValueRequest().withSecretId(secretName);
        GetSecretValueResult getSecretValueResult = client.getSecretValue(getSecretValueRequest);

        if (getSecretValueResult.getSecretString() != null) {secret = getSecretValueResult.getSecretString();}
        else {String decodedBinarySecret = new String(Base64.getDecoder().decode(getSecretValueResult.getSecretBinary()).array());
            Log.info(decodedBinarySecret);}

        JSONParser parser = new JSONParser();
        try {object = (JSONObject) parser.parse(secret);}
        catch (ParseException e) {e.printStackTrace();}
        return object;
    }
}
