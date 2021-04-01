package com.eph.automation.testing.services.api;
//updated by Nishant @ 30 Mar 2021 for secret manager EPHD-3045

import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.api.AccessToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import javax.ws.rs.core.Response.Status;
import net.minidev.json.JSONObject;
import jdk.nashorn.internal.ir.ObjectNode;
import org.apache.http.NameValuePair;
import org.apache.http.StatusLine;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;

import static com.eph.automation.testing.configuration.SecretsManagerHandler.getSecretKeyObj;

public class AuthorizationService {
    private static AccessToken token;
    private static JSONObject secretObject;

//    public static String getAuthToken() {
//        RestAssured.useRelaxedHTTPSValidation();
//        RequestSpecification request = RestAssured.with();
//        return request.baseUri(Constants.CUSTOMER_SERVICE_URL)
//                .basePath(Constants.GET_TOKEN_PATH)
//                .header(Constants.AUTHORIZATION_HEADER, Constants.CUSTOMER_GATEWAY_AUTHORIZATION_HEADER_VALUE)
//                .queryParam(Constants.GRANT_TYPE, Constants.GRANT_TYPE_VALUE)
//                .post()
//                .then()
//                .statusCode(200)
//                .extract().path("access_token");
//    }

    public static synchronized AccessToken getAuthToken() throws AzureOauthTokenFetchingException
    {
        if(secretObject==null){secretObject=getSecretKeyObj("eu-west-1","eph_api_oauth2_token");};


        if ((token == null) || (!token.isValid(Long.valueOf(secretObject.getAsString("expiryOffsetSeconds")))))
        {
            Log.info("Oauth Token being Requested");
            token = getAzureAccessTokenFetch();
        }
        else
        {
            Log.info("Oauth Token cached version used");
        }

        if (!token.isValid(Long.valueOf(secretObject.getAsString("expiryOffsetSeconds"))))
        {
            throw new AzureOauthTokenFetchingException("Could not get a valid token, expiry: " + token.getExpiresOnAsString() + " offset: " + secretObject.getAsString("expiryOffsetSeconds"));
        }

        return token;
    }

    public static AccessToken getAzureAccessTokenFetch() throws AzureOauthTokenFetchingException
    {
        String responseBody =  makeRequestAndGetResponseBody();
        AccessToken accessToken = convertResponseToToken(responseBody);

//        Log.debug("Got: {}", accessToken);
        return accessToken;
    }

    private static String makeRequestAndGetResponseBody() throws AzureOauthTokenFetchingException
    {//Data
        String responseString = null;
        CloseableHttpClient client = null;
        CloseableHttpResponse response = null;
        try
        {
            URI tokenRequestUri = null;
            try
            {
                tokenRequestUri = URI.create(
                        secretObject.getAsString("uriPrefix") +
                                secretObject.getAsString("tenantId") +
                                secretObject.getAsString("uriPostfix"));
            }
            catch (Exception e)
            {
                throw new AzureOauthTokenFetchingException("Error creating URI from: " +
                        secretObject.getAsString("uriPrefix") +
                        secretObject.getAsString("tenantId") +
                        secretObject.getAsString("uriPostfix"), e);

            }

            RequestConfig config = RequestConfig.custom()
                    .setConnectTimeout(Integer.parseInt(secretObject.getAsString("httptimeoutmilliseconds")))
                    .setConnectionRequestTimeout(Integer.parseInt(secretObject.getAsString("httptimeoutmilliseconds")))
                    .setSocketTimeout(Integer.parseInt(secretObject.getAsString("httptimeoutmilliseconds")))
                    .build();
            client = HttpClientBuilder.create().setDefaultRequestConfig(config).build();

            HttpPost httpPost = new HttpPost(tokenRequestUri);
            List<NameValuePair> params = new ArrayList<>();
            params.add(new BasicNameValuePair("grant_type", "client_credentials"));
            params.add(new BasicNameValuePair("client_id", secretObject.getAsString("clientId")));
            params.add(new BasicNameValuePair("client_secret", secretObject.getAsString("clientSecret")));
            httpPost.setEntity(new UrlEncodedFormEntity(params,StandardCharsets.UTF_8));

            try
            {
                response = client.execute(httpPost);
            }
            catch (IOException e)
            {
                throw new AzureOauthTokenFetchingException("Error calling/connecting to Oauth service",e);
            }

            StatusLine statusLine = response.getStatusLine();
            if ((statusLine == null) || (Status.OK.getStatusCode() != statusLine.getStatusCode()))
            {
                try
                {
                    responseString = new BasicResponseHandler().handleResponse(response);
                }
                catch (Exception e)
                {
                    // Do nothing already in an error path
                }
                throw new AzureOauthTokenFetchingException("Unexpected response from HTTP service: " + statusLine + ", message body: " + responseString);
            }

            try
            {
                responseString = new BasicResponseHandler().handleResponse(response);
            }
            catch (IOException e)
            {
                throw new AzureOauthTokenFetchingException("Error reading response from Oauth service",e);
            }

        }
        finally
        {
            closeHttpComponents(client, response);
        }
        return responseString;
    }

    private static AccessToken convertResponseToToken(String response) throws AzureOauthTokenFetchingException
    {
        AccessToken accessToken = null;
        ObjectMapper mapper = new ObjectMapper();

        try
        {
            //updated by Nishant @ 20 Nov 2019
            accessToken = mapper.readValue(response, AccessToken.class);
            JsonNode node = mapper.readTree(response);
            String access_token = node.get("access_token").textValue();
            String temp = accessToken.getToken();
        }
        catch (IOException e)
        {
            throw new AzureOauthTokenFetchingException("Error parsing response body: " + response, e);
        }

        return accessToken;
    }

    public static void closeHttpComponents(CloseableHttpClient client, CloseableHttpResponse response)
    {
        if (response != null)
        {
            try
            {
                response.close();
            }
            catch (IOException e)
            {
                Log.info("Error thrown when closing HTTP client's response object");
            }
        }
        if (client != null)
        {
            try
            {
                client.close();
            }
            catch (IOException e)
            {
                Log.info("Error thrown when closing HTTP client");
            }
        }
    }


}
