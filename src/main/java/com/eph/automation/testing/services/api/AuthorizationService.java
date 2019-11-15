package com.eph.automation.testing.services.api;

import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.api.AccessToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.core.Response.Status;

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

import static com.eph.automation.testing.configuration.Constants.*;


public class AuthorizationService {
    private static AccessToken token;


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
        if ((token == null) || (!token.isValid(Long.valueOf(expiryOffsetSeconds))))
        {
            Log.info("Oauth Token being Requested");
            token = getAzureAccessTokenFetch();
        }
        else
        {
            Log.info("Oauth Token cached version used");
        }

        if (!token.isValid(Long.valueOf(expiryOffsetSeconds)))
        {
            throw new AzureOauthTokenFetchingException("Could not get a valid token, expiry: " + token.getExpiresOnAsString() + " offset: " + expiryOffsetSeconds);
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
    {Data
        String responseString = null;
        CloseableHttpClient client = null;
        CloseableHttpResponse response = null;
        try
        {
            URI tokenRequestUri = null;
            try
            {
                tokenRequestUri = URI.create(uriPrefix + tenantId + uriPostfix);
            }
            catch (Exception e)
            {
                throw new AzureOauthTokenFetchingException("Error creating URI from: " + uriPrefix + tenantId + uriPostfix, e);

            }

            RequestConfig config = RequestConfig.custom()
                    .setConnectTimeout(Integer.parseInt(httptimeoutmilliseconds))
                    .setConnectionRequestTimeout(Integer.parseInt(httptimeoutmilliseconds))
                    .setSocketTimeout(Integer.parseInt(httptimeoutmilliseconds))
                    .build();
            client = HttpClientBuilder.create().setDefaultRequestConfig(config).build();

            HttpPost httpPost = new HttpPost(tokenRequestUri);
            List<NameValuePair> params = new ArrayList<>();
            params.add(new BasicNameValuePair("grant_type", "client_credentials"));
            params.add(new BasicNameValuePair("client_id", clientId));
            params.add(new BasicNameValuePair("client_secret", clientSecret));
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
            accessToken = mapper.readValue(response, AccessToken.class);
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
