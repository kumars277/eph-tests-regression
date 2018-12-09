package com.eph.automation.testing.configuration;

/**
 * Created by RAVIVARMANS on 11/24/2018.
 */
public class Constants {

    //UI End Points
    public static String SIT_EPH_UI = "https://sit-eph.app.science.regn.net/semarchy/welcome";
    public static String THREE_S_BUCKET_UI = "https://federation.reedelsevier.com/adfs/ls/IdpInitiatedSignOn.aspx?loginToRp=urn:amazon:webservices";

    //SOAP EIP - SIT_CRM End point
    public static String EIP_CMX_CUSTOMER_MAINTENANCE_END_POINT = "https://enterprise-sit.api.elsevier.com/cxf/customerMaintenanceDS/v2/?wsdl";
    public static String EIP_CMX_CUSTOMER_MAINTENANCE_END_POINT_UAT = "https://enterprise-uat.api.elsevier.com/cxf/customerMaintenanceDS/v2/?wsdl";
    public static final String EIP_NOTIFICATION_WADL_END_POINT_SIT = "https://sit.ifp.elsevier.com/cxf/CustomerNotifications/?_wadl";

    //New SIT_CRM EMS Notification End points
    public static final String EIP_NOTIFICATION_ORG_CRM_END_POINT_SIT = "https://enterprise-sit.api.elsevier.com";
    public static final String EIP_NOTIFICATION_ORG_SDW_END_POINT_SIT = "https://customer-notification-sdw-service-eipsit.apps.ose-nonprod.cp.tio.systems";

    //PPM Database Constants
    public static String PPM_UAT_URL = "PPM_UAT_URL";
    public static String PPM_DEV_URL = "PPM_DEV_URL";
    public static String PPM_PROD_URL = "PPM_PROD_URL";
    public static String PPM_UAT_SCHEMA = "MEX_OPER_DEV";
    public static String PPM_PROD_SCHEMA = "MEX_OPER_PROD";

    //PMX Database Constants
    public static String PMX_SIT_URL = "PMX_SIT_URL";
    public static String PMX_UAT_URL = "PMX_UAT_URL";
    public static String PMX_PROD_URL = "PMX_PROD_URL";

    //EPH Database Constants
    public static String EPH_SIT_URL = "EPH_SIT_URL";


    //MySql Database
    public static String MYSQL_SIT_DB_URL_KEY = "MYSQL_SIT_URL";
    public static String MYSQL_UAT_DB_URL_KEY = "MYSQL_UAT_URL";

    public static String HEADER_KEY_FOR_EIP_NOTIFICAIOTN = "Authorization";
    public static String HEADER_VALUE_FOR_EIP_NOTIFICAIOTN = "Basic Q01YX1RETVBfVVNFUjAwMTpEcnVNLVVsdGltYXRFLU1ha0UtQ29tbXVuaXRpZVM=";

    public static String ENCRYPTION_PASSWORD = "encryptionPassword";

    //Wiremock
    public static String WIRE_MOCK_END_POINT = "http://10.48.67.42:8080/api/";
}
