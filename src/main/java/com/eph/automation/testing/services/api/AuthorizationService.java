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


//    public Boolean getBoolTRIAL_ALLOWED_IND() {
//        if(TRIAL_ALLOWED_IND.equalsIgnoreCase("t")) {
//            return true;
//        } else {
//            return false;
//        }
//    }
//
//    public Boolean getBoolSEPARATELY_SALEABLE_IND() {
//        if(SEPARATELY_SALEABLE_IND.equalsIgnoreCase("t")) {
//            return true;
//        } else {
//            return false;
//        }
//    }

    public static String SELECT_RANDOM_PRODUCT_IDS_FOR_SEARCH_BOOKS = "SELECT \"product_id\" as PRODUCT_ID\n" +
            "FROM semarchy_eph_mdm.gd_product\n" +
            "WHERE exists\n" +
            "    (SELECT * \n" +
            "     from semarchy_eph_stg.st_out_notification\n" +
            "     WHERE semarchy_eph_mdm.gd_product.product_id = semarchy_eph_stg.st_out_notification.notification_id and semarchy_eph_stg.st_out_notification.status='PROCESSED') \n" +
            "order by random() limit '%s'";

    public static String SELECT_RANDOM_WORK_IDS_FOR_SEARCH = "SELECT \"work_id\" as WORK_ID\n" +
            "FROM semarchy_eph_mdm.gd_wwork " +
            "WHERE exists (SELECT * FROM semarchy_eph_stg.st_out_notification " +
            "WHERE semarchy_eph_mdm.gd_wwork.work_id = semarchy_eph_stg.st_out_notification.notification_id and semarchy_eph_stg.st_out_notification.status='PROCESSED')" +
            "order by random() limit '%s'";

    public static String EPH_GD_PRODUCT_EXTRACT_FOR_SEARCH = "SELECT \n" +
            "              product_id AS PRODUCT_ID -- Title\n" +
            "              ,name as PRODUCT_NAME\n" +
            "              ,separately_sale_indicator as SEPARATELY_SALEABLE_IND\n" +
            "              ,trial_allowed_indicator as TRIAL_ALLOWED_IND\n" +
            "              ,short_name as PRODUCT_SHORT_NAME\n" +
            "              ,launch_date as FIRST_PUB_DATE\n" +
            "              ,f_type AS F_TYPE\n" +
            "              ,f_status AS F_STATUS\n" +
            "              ,f_revenue_model AS F_REVENUE_MODEL\n" +
            "              ,f_wwork AS F_PRODUCT_WORK\n" +
            "              ,f_manifestation AS F_PRODUCT_MANIFESTATION_TYP\n" +
            "              FROM ephsit.semarchy_eph_mdm.gd_product\n" +
            "  WHERE product_id IN ('%s')";

    public static String EPH_GD_WORK_EXTRACT_FOR_SEARCH = "SELECT \n" +
            "               work_id AS WORK_ID,\n" +
            "              work_title AS WORK_TITLE,\n" +
            "               work_sub_title AS WORK_SUBTITLE,\n" +
            "               volume AS VOLUME,\n" +
            "               electro_rights_indicator AS ELECTRONIC_RIGHTS_IND,\n" +
            "               f_pmc AS PMC,\n" +
            "               copyright_year as COPYRIGHT_YEAR,\n" +
            "               f_status AS WORK_STATUS,\n" +
            "               f_type AS F_TYPE,\n" +
            "               f_imprint AS IMPRINT\n" +
            "              FROM ephsit.semarchy_eph_mdm.gd_wwork " +
            "  WHERE work_id IN ('%s')";
}
