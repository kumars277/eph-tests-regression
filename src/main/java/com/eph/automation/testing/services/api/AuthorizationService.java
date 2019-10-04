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
//        return request.baseUri(Constants.CUSTOMER_SERVICE_URL)
//                .basePath(Constants.GET_TOKEN_PATH)
//                .header(Constants.AUTHORIZATION_HEADER, Constants.CUSTOMER_GATEWAY_AUTHORIZATION_HEADER_VALUE)
//                .queryParam(Constants.GRANT_TYPE, Constants.GRANT_TYPE_VALUE)
//                .post()
//                .then()
//                .statusCode(200)
//                .extract().path("access_token");

        return "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6ImFQY3R3X29kdlJPb0VOZzNWb09sSWgydGlFcyIsImtpZCI6ImFQY3R3X29kdlJPb0VOZzNWb09sSWgydGlFcyJ9.eyJhdWQiOiI5OWI5ZDY1MS1jODhjLTRkNjgtYmQxYy02Y2JlZTYzOWM3MjEiLCJpc3MiOiJodHRwczovL3N0cy53aW5kb3dzLm5ldC85Mjc0ZWUzZi05NDI1LTQxMDktYTI3Zi05ZmIxNWMxMDY3NWQvIiwiaWF0IjoxNTY5NTY5MjQ2LCJuYmYiOjE1Njk1NjkyNDYsImV4cCI6MTU2OTU3MzE0NiwiYWlvIjoiNDJGZ1lKQkpaR0pkdWF4eWJzanBTLzhUK0hsTzlRb21Pazk1YVhQdTVRTUZtMGJMbWg4QSIsImFtciI6WyJ3aWEiXSwiZmFtaWx5X25hbWUiOiJEcmF6aGV2YSIsImdpdmVuX25hbWUiOiJCaXN0cmEiLCJpcGFkZHIiOiIxOTguMTc2LjgyLjMzIiwibmFtZSI6IkRyYXpoZXZhLCBCaXN0cmEgKEVMUykiLCJub25jZSI6ImViMDgyOWQyLTI3YzgtNDJmYS1hNjFlLTA2N2U3MzJmMWQwNiIsIm9pZCI6ImYzMTdjNjUzLTA4NTMtNDdhMS05MTYyLWQ2Y2Q1NTY3YTFkNSIsIm9ucHJlbV9zaWQiOiJTLTEtNS0yMS0xNjA2OTgwODQ4LTQ4NDc2Mzg2OS03MjUzNDU1NDMtMzE1MjkwIiwicm9sZXMiOlsiUHJvZHVjdEh1YlNlYXJjaCJdLCJzdWIiOiJBbUFkeHBoMGlNWloyMFBHbWpqMHUzU3YwUDQ5TEkzZHR6dkVNSU1WMG00IiwidGlkIjoiOTI3NGVlM2YtOTQyNS00MTA5LWEyN2YtOWZiMTVjMTA2NzVkIiwidW5pcXVlX25hbWUiOiJEUkFaSEVWQUJAc2NpZW5jZS5yZWduLm5ldCIsInVwbiI6IkRSQVpIRVZBQkBzY2llbmNlLnJlZ24ubmV0IiwidXRpIjoidWh4UG00ZEFNRUNTeDdDTl81azBBQSIsInZlciI6IjEuMCJ9.bQ8A5iiGRx6dNKcu9NJEqhsQvKeBaH6qBHEPBBs-wvqkRhcW8INlfuRBrVoOZq7LYs32vPxirgOmgyjpS3WvR2jZUEB2pmHugBwtYLdU4CWUodUPvRBUGCDnJE9Us7GEyXNFEXdhzLAItR0Vub282m_vImnHujdppQQyY0VjQmoALZcoVXsEPSn-kDJAAmie5m8kiLEIuxYk2-5J-i9eV-PaBJ4au4QS7KGTx_EFiFWJQNTY3kkWVAFr-rT5CWT7lt2K4AKLJk8pJASt4LEXT0Jb3Nl-Bzsrhvl3RF79JKMkBbB85IPvfS39kor8D_N0KvNEJZHm2dAb88a62QYI4w";
    }
}
