package com.eph.automation.testing.services.api;

import com.eph.automation.testing.configuration.Constants;
import com.jayway.restassured.RestAssured;
import com.jayway.restassured.specification.RequestSpecification;

/**
 * Created by NenkovaL on 12/10/2018.
 */
public class AuthorizationService {
    public static String getAuthToken() {
        RestAssured.useRelaxedHTTPSValidation();
        RequestSpecification request = RestAssured.with();
        return request.baseUri(Constants.CUSTOMER_SERVICE_URL)
                .basePath(Constants.GET_TOKEN_PATH)
                .header(Constants.AUTHORIZATION_HEADER, Constants.CUSTOMER_GATEWAY_AUTHORIZATION_HEADER_VALUE)
                .queryParam(Constants.GRANT_TYPE, Constants.GRANT_TYPE_VALUE)
                .post()
                .then()
                .statusCode(200)
                .extract().path("access_token");
    }
}
