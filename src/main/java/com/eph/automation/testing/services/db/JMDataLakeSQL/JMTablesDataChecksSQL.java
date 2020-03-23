package com.eph.automation.testing.services.db.JMDataLakeSQL;

import com.eph.automation.testing.configuration.Constants;

public class JMTablesDataChecksSQL {

    public static String GET_ALLOCATION_IDS = "select ALLOCATION_CHANGE_ID from " + Constants.JMF_SQL_SCHEMA + ".JMF_ALLOCATION_CHANGE order by rand() limit %s";

    public static String GET_APPLICATION_KEY = "select APPLICATION_PROPERTY_KEY from " + Constants.JMF_SQL_SCHEMA + ".JMF_APPLICATION_PROPERTIES order by rand() limit %s";

    public static String GET_APPROVAL_REQ_ID = "select APPROVAL_ID from " + Constants.JMF_SQL_SCHEMA + ".JMF_APPROVAL_REQUEST order by rand() limit %s";

    public static String GET_CHRONICLE_SCE_ID = "select CHRONICLE_SCENARIO_CODE from " + Constants.JMF_SQL_SCHEMA + ".JMF_CHRONICLE_SCENARIO order by rand() limit %s";

    public static String GET_CHRONICLE_STATUS_ID = "select CHRONICLE_STATUS_CODE from " + Constants.JMF_SQL_SCHEMA + ".JMF_CHRONICLE_STATUS order by rand() limit %s";

    public static String GET_FAMILY_RESOURCE_ID = "select FAMILY_RESOURCE_DETAILS_ID from " + Constants.JMF_SQL_SCHEMA + ".JMF_FAMILY_RESOURCE_DETAILS order by rand() limit %s";

    public static String GET_PRODUCT_WORK_ID = "select PRODUCT_WORK_ID from " + Constants.JMF_SQL_SCHEMA + ".JMF_FINANCIAL_INFORMATION order by rand() limit %s";

    public static String GET_LEGAL_PRODUCT_WORK_ID = "select PRODUCT_WORK_ID from " + Constants.JMF_SQL_SCHEMA + ".JMF_LEGAL_INFORMATION order by rand() limit %s";

    public static String GET_MANIF_PRODUCT_ID = "select PRODUCT_MANIFESTATION_ID from " + Constants.JMF_SQL_SCHEMA + ".JMF_MANIFESTATION_ELECTRONIC_DETAILS order by rand() limit %s";

    public static String GET_MANIF_PRINT_ID = "select PRODUCT_MANIFESTATION_ID from " + Constants.JMF_SQL_SCHEMA + ".JMF_MANIFESTATION_PRINT_DETAILS order by rand() limit %s";

    public static String GET_PARTY_PROD_ID = "select PARTY_IN_PRODUCT_ID from " + Constants.JMF_SQL_SCHEMA + ".JMF_PARTY_IN_PRODUCT order by rand() limit %s";

    public static String GET_PRODUCT_AVAIL_ID = "select PRODUCT_AVAILABILITY_ID from " + Constants.JMF_SQL_SCHEMA + ".JMF_PRODUCT_AVAILABILITY order by rand() limit %s";


    public static String getAllocationChangesSql(String serverEnv, String table) {
        String GET_DATA_ALLOCATION_CHANGE_JM = null;
        switch (serverEnv) {
            case ("mysql"):
                GET_DATA_ALLOCATION_CHANGE_JM = "select \n" +
                        "ALLOCATION_CHANGE_ID as ALLOCATION_CHANGE_ID\n" +
                        ",ALLOCATION_TYPE as ALLOCATION_TYPE\n" +
                        ",PMG_FILTER as PMG_FILTER\n" +
                        ",PMC_FILTER as PMC_FILTER\n" +
                        ",PPC_FILTER_EMAIL as PPC_FILTER_EMAIL\n" +
                        ",PPC_FILTER_NAME as PPC_FILTER_NAME\n" +
                        ",PMC_CHANGE_IND as PMC_CHANGE_IND\n" +
                        ",PPC_CHANGE_IND as PPC_CHANGE_IND\n" +
                        ",PMX_PMGCODE as PMX_PMGCODE\n" +
                        ",PMX_PMG_NAME as PMX_PMG_NAME\n" +
                        ",PMX_PMG_F_BUSINESS_UNIT as PMX_PMG_F_BUSINESS_UNIT\n" +
                        ",PMG_BUS_CTRL_NAME as PMG_BUS_CTRL_NAME\n" +
                        ",PMG_BUS_CTRL_EMAIL as PMG_BUS_CTRL_EMAIL\n" +
                        ",PMG_PUBDIR_NAME_OLD as PMG_PUBDIR_NAME_OLD\n" +
                        ",PMG_PUBDIR_EMAIL_OLD as PMG_PUBDIR_EMAIL_OLD\n" +
                        ",PMG_PUBDIR_PEOPLE_HUB_ID_OLD as PMG_PUBDIR_PEOPLE_HUB_ID_OLD\n" +
                        ",PMG_PUBDIR_NAME_NEW as PMG_PUBDIR_NAME_NEW\n" +
                        ",PMG_PUBDIR_EMAIL_NEW as PMG_PUBDIR_EMAIL_NEW\n" +
                        ",PMG_PUBDIR_PEOPLE_HUB_ID_NEW as PMG_PUBDIR_PEOPLE_HUB_ID_NEW\n" +
                        ",NOTIFIED_DATE as NOTIFIED_DATE\n" +
                        ",EPH_PMG_CODE asEPH_PMG_CODE\n" +
                        "from " + Constants.JMF_SQL_SCHEMA + "." + table + "\n" +
                        " where ALLOCATION_CHANGE_ID in ('%s')";
                break;
            case ("aws"):
                GET_DATA_ALLOCATION_CHANGE_JM = "select \n" +
                        "ALLOCATION_CHANGE_ID as ALLOCATION_CHANGE_ID\n" +
                        ",ALLOCATION_TYPE as ALLOCATION_TYPE\n" +
                        ",PMG_FILTER as PMG_FILTER\n" +
                        ",PMC_FILTER as PMC_FILTER\n" +
                        ",PPC_FILTER_EMAIL as PPC_FILTER_EMAIL\n" +
                        ",PPC_FILTER_NAME as PPC_FILTER_NAME\n" +
                        ",PMC_CHANGE_IND as PMC_CHANGE_IND\n" +
                        ",PPC_CHANGE_IND as PPC_CHANGE_IND\n" +
                        ",PMX_PMGCODE as PMX_PMGCODE\n" +
                        ",PMX_PMG_NAME as PMX_PMG_NAME\n" +
                        ",PMX_PMG_F_BUSINESS_UNIT as PMX_PMG_F_BUSINESS_UNIT\n" +
                        ",PMG_BUS_CTRL_NAME as PMG_BUS_CTRL_NAME\n" +
                        ",PMG_BUS_CTRL_EMAIL as PMG_BUS_CTRL_EMAIL\n" +
                        ",PMG_PUBDIR_NAME_OLD as PMG_PUBDIR_NAME_OLD\n" +
                        ",PMG_PUBDIR_EMAIL_OLD as PMG_PUBDIR_EMAIL_OLD\n" +
                        ",PMG_PUBDIR_PEOPLE_HUB_ID_OLD as PMG_PUBDIR_PEOPLE_HUB_ID_OLD\n" +
                        ",PMG_PUBDIR_NAME_NEW as PMG_PUBDIR_NAME_NEW\n" +
                        ",PMG_PUBDIR_EMAIL_NEW as PMG_PUBDIR_EMAIL_NEW\n" +
                        ",PMG_PUBDIR_PEOPLE_HUB_ID_NEW as PMG_PUBDIR_PEOPLE_HUB_ID_NEW\n" +
                        ",NOTIFIED_DATE as NOTIFIED_DATE\n" +
                        ",EPH_PMG_CODE asEPH_PMG_CODE\n" +
                        "from " + Constants.JMF_AWS_SCHEMA + "." + table + "\n" +
                        " where inbound_ts = (select inbound_ts from " + Constants.JMF_AWS_SCHEMA + "." + table + " order by inbound_ts desc limit 1)\n" +
                        " AND ALLOCATION_CHANGE_ID in ('%s')";
                break;
        }
        return GET_DATA_ALLOCATION_CHANGE_JM;

    }


    public static String getAppPropertySql(String serverEnv, String table) {
        String GET_DATA_APP_PROP_JM = null;
        switch (serverEnv) {
            case ("mysql"):
                GET_DATA_APP_PROP_JM = "select \n" +
                        "APPLICATION_PROPERTY_KEY as APPLICATION_PROPERTY_KEY\n" +
                        ",APPLICATION_PROPERTY_VALUE as APPLICATION_PROPERTY_VALUE\n" +
                        ",NOTIFIED_DATE as NOTIFIED_DATE\n" +
                        "from " + Constants.JMF_SQL_SCHEMA + "." + table + "\n" +
                        " where APPLICATION_PROPERTY_KEY in ('%s')";
                break;
            case ("aws"):
                GET_DATA_APP_PROP_JM = "select \n" +
                        "APPLICATION_PROPERTY_KEY as APPLICATION_PROPERTY_KEY\n" +
                        ",APPLICATION_PROPERTY_VALUE as APPLICATION_PROPERTY_VALUE\n" +
                        ",NOTIFIED_DATE as NOTIFIED_DATE\n" +
                        "from " + Constants.JMF_AWS_SCHEMA + "." + table + "\n" +
                        " where inbound_ts = (select inbound_ts from " + Constants.JMF_AWS_SCHEMA + "." + table + " order by inbound_ts desc limit 1)\n" +
                        " AND APPLICATION_PROPERTY_KEY in ('%s')";
                break;

        }
        return GET_DATA_APP_PROP_JM;

    }

    public static String getAppRequestSql(String serverEnv, String table) {
        String GET_DATA_APP_REQ_JM = null;
        switch (serverEnv) {
            case ("mysql"):
                GET_DATA_APP_REQ_JM = "select \n" +
                        "APPROVAL_ID as APPROVAL_ID\n" +
                        ",F_PRODUCT_CHRONICLE as F_PRODUCT_CHRONICLE\n" +
                        ",APPROVAL_TYPE as APPROVAL_TYPE\n" +
                        ",APPROVER_NAME as APPROVER_NAME\n" +
                        ",APPROVAL_GIVEN_INDICATOR as APPROVAL_GIVEN_INDICATOR\n" +
                        ",APPROVAL_REQUEST_DATE as APPROVAL_REQUEST_DATE\n" +
                        ",APPROVAL_RESPONSE_DATE as APPROVAL_RESPONSE_DATE\n" +
                        "from " + Constants.JMF_SQL_SCHEMA + "." + table + "\n" +
                        " where APPROVAL_ID in ('%s')";
                break;
            case ("aws"):
                GET_DATA_APP_REQ_JM = "select \n" +
                        "APPROVAL_ID as APPROVAL_ID\n" +
                        ",F_PRODUCT_CHRONICLE as F_PRODUCT_CHRONICLE\n" +
                        ",APPROVAL_TYPE as APPROVAL_TYPE\n" +
                        ",APPROVER_NAME as APPROVER_NAME\n" +
                        ",APPROVAL_GIVEN_INDICATOR as APPROVAL_GIVEN_INDICATOR\n" +
                        ",APPROVAL_REQUEST_DATE as APPROVAL_REQUEST_DATE\n" +
                        ",APPROVAL_RESPONSE_DATE as APPROVAL_RESPONSE_DATE\n" +
                        "from " + Constants.JMF_AWS_SCHEMA + "." + table + "\n" +
                        " where inbound_ts = (select inbound_ts from " + Constants.JMF_AWS_SCHEMA + "." + table + " order by inbound_ts desc limit 1)\n" +
                        " AND APPROVAL_ID in ('%s')";
                break;

        }
        return GET_DATA_APP_REQ_JM;
    }

    public static String getChronicleSceSql(String serverEnv, String table) {
        String GET_DATA_CHRO_SCE_JM = null;
        switch (serverEnv) {
            case ("mysql"):
                GET_DATA_CHRO_SCE_JM = "select \n" +
                        "CHRONICLE_SCENARIO_CODE as CHRONICLE_SCENARIO_CODE\n" +
                        ",CHRONICLE_SCENARIO_NAME as CHRONICLE_SCENARIO_NAME\n" +
                        ",CHRONICLE_SCENARIO_DESCRIPTION as CHRONICLE_SCENARIO_DESCRIPTION\n" +
                        ",CHRONICLE_SCENARIO_EVOLUTIONARY_IND as CHRONICLE_SCENARIO_EVOLUTIONARY_IND\n" +
                        ",NOTIFIED_DATE as NOTIFIED_DATE\n" +
                        "from " + Constants.JMF_SQL_SCHEMA + "." + table + "\n" +
                        " where CHRONICLE_SCENARIO_CODE in ('%s')";
                break;
            case ("aws"):
                GET_DATA_CHRO_SCE_JM = "select \n" +
                        "CHRONICLE_SCENARIO_CODE as CHRONICLE_SCENARIO_CODE\n" +
                        ",CHRONICLE_SCENARIO_NAME as CHRONICLE_SCENARIO_NAME\n" +
                        ",CHRONICLE_SCENARIO_DESCRIPTION as CHRONICLE_SCENARIO_DESCRIPTION\n" +
                        ",CHRONICLE_SCENARIO_EVOLUTIONARY_IND as CHRONICLE_SCENARIO_EVOLUTIONARY_IND\n" +
                        ",NOTIFIED_DATE as NOTIFIED_DATE\n" +
                        "from " + Constants.JMF_AWS_SCHEMA + "." + table + "\n" +
                        " where inbound_ts = (select inbound_ts from " + Constants.JMF_AWS_SCHEMA + "." + table + " order by inbound_ts desc limit 1)\n" +
                        " AND CHRONICLE_SCENARIO_CODE in ('%s')";
                break;

        }
        return GET_DATA_CHRO_SCE_JM;

    }

    public static String getChronicleStatusSql(String serverEnv, String table) {
        String GET_DATA_CHRO_STATUS_JM = null;
        switch (serverEnv) {
            case ("mysql"):
                GET_DATA_CHRO_STATUS_JM = "select \n" +
                        "CHRONICLE_STATUS_CODE as CHRONICLE_STATUS_CODE\n" +
                        ",CHRONICLE_STATUS_NAME as CHRONICLE_STATUS_NAME\n" +
                        ",CHRONICLE_STATUS_DESCRIPTION as CHRONICLE_STATUS_DESCRIPTION\n" +
                        ",NOTIFIED_DATE as NOTIFIED_DATE\n" +
                        "from " + Constants.JMF_SQL_SCHEMA + "." + table + "\n" +
                        " where CHRONICLE_STATUS_CODE in ('%s')";
                break;
            case ("aws"):
                GET_DATA_CHRO_STATUS_JM = "select \n" +
                        "CHRONICLE_STATUS_CODE as CHRONICLE_STATUS_CODE\n" +
                        ",CHRONICLE_STATUS_NAME as CHRONICLE_STATUS_NAME\n" +
                        ",CHRONICLE_STATUS_DESCRIPTION as CHRONICLE_STATUS_DESCRIPTION\n" +
                        ",NOTIFIED_DATE as NOTIFIED_DATE\n" +
                        "from " + Constants.JMF_AWS_SCHEMA + "." + table + "\n" +
                        " where inbound_ts = (select inbound_ts from " + Constants.JMF_AWS_SCHEMA + "." + table + " order by inbound_ts desc limit 1)\n" +
                        " AND CHRONICLE_STATUS_CODE in ('%s')";
                break;

        }
        return GET_DATA_CHRO_STATUS_JM;

    }

    public static String getFamilyResourceSql(String serverEnv, String table) {
        String GET_DATA_FAMILY_RESOURCE_JM = null;
        switch (serverEnv) {
            case ("mysql"):
                GET_DATA_FAMILY_RESOURCE_JM = "select \n" +
                        "FAMILY_RESOURCE_DETAILS_ID as FAMILY_RESOURCE_DETAILS_ID\n" +
                        ",RESOURCE_KEY as RESOURCE_KEY\n" +
                        ",INITIAL_VALUE as INITIAL_VALUE\n" +
                        ",RESOURCE_CHANGE_INDICATOR as RESOURCE_CHANGE_INDICATOR\n" +
                        ",NEW_VALUE as NEW_VALUE\n" +
                        ",F_PRODUCT_FAMILY as F_PRODUCT_FAMILY\n" +
                        ",PEOPLEHUB_ID_OLD as PEOPLEHUB_ID_OLD\n" +
                        ",PEOPLEHUB_ID_NEW as PEOPLEHUB_ID_NEW\n" +
                        ",NOTIFIED_DATE as NOTIFIED_DATE\n" +
                        "from " + Constants.JMF_SQL_SCHEMA + "." + table + "\n" +
                        " where FAMILY_RESOURCE_DETAILS_ID in ('%s')";
                break;
            case ("aws"):
                GET_DATA_FAMILY_RESOURCE_JM = "select \n" +
                        "FAMILY_RESOURCE_DETAILS_ID as FAMILY_RESOURCE_DETAILS_ID\n" +
                        ",RESOURCE_KEY as RESOURCE_KEY\n" +
                        ",INITIAL_VALUE as INITIAL_VALUE\n" +
                        ",RESOURCE_CHANGE_INDICATOR as RESOURCE_CHANGE_INDICATOR\n" +
                        ",NEW_VALUE as NEW_VALUE\n" +
                        ",F_PRODUCT_FAMILY as F_PRODUCT_FAMILY\n" +
                        ",PEOPLEHUB_ID_OLD as PEOPLEHUB_ID_OLD\n" +
                        ",PEOPLEHUB_ID_NEW as PEOPLEHUB_ID_NEW\n" +
                        ",NOTIFIED_DATE as NOTIFIED_DATE\n" +
                        "from " + Constants.JMF_AWS_SCHEMA + "." + table + "\n" +
                        " where inbound_ts = (select inbound_ts from " + Constants.JMF_AWS_SCHEMA + "." + table + " order by inbound_ts desc limit 1)\n" +
                        " AND FAMILY_RESOURCE_DETAILS_ID in ('%s')";
                break;

        }
        return GET_DATA_FAMILY_RESOURCE_JM;

    }

    public static String getFinancialInfoSql(String serverEnv, String table) {
        String GET_DATA_FINANCE_INFO_JM = null;
        switch (serverEnv) {
            case ("mysql"):
                GET_DATA_FINANCE_INFO_JM = "select \n" +
                        "PRODUCT_WORK_ID as PRODUCT_WORK_ID\n" +
                        ",BUSINESS_CONTROLLER_NAME as BUSINESS_CONTROLLER_NAME\n" +
                        ",BUSINESS_UNIT_CODE as BUSINESS_UNIT_CODE\n" +
                        ",PMG_CODE as PMG_CODE\n" +
                        ",PMC_CODE as PMC_CODE\n" +
                        ",OPCO_R12_ID as OPCO_R12_ID\n" +
                        ",OPCO_R12_CODE as OPCO_R12_CODE\n" +
                        ",CONTRACT_OPCO_R12_CODE as CONTRACT_OPCO_R12_CODE\n" +
                        ",RESPONSIBILITY_CENTRE_CODE as RESPONSIBILITY_CENTRE_CODE\n" +
                        ",HFM_HIERARCHY_POSITION_CODE as HFM_HIERARCHY_POSITION_CODE\n" +
                        ",CITY_OPCO_R12_CODE as CITY_OPCO_R12_CODE\n" +
                        ",COUNTRY_OPCO_R12_CODE as COUNTRY_OPCO_R12_CODE\n" +
                        ",OPCO_R12_NAME as OPCO_R12_NAME\n" +
                        ",OPCO_R12_LEGAL_NAME as OPCO_R12_LEGAL_NAME\n" +
                        ",REFUND_OPTION as REFUND_OPTION\n" +
                        ",REMINDER_DATE as REMINDER_DATE\n" +
                        ",REMINDER_OPTION as REMINDER_OPTION\n" +
                        ",RENEWAL_OPTION as RENEWAL_OPTION\n" +
                        ",RENEWAL_DATE as RENEWAL_DATE\n" +
                        ",BUSINESS_CONTROLLER_EMAIL_ADDRESS as BUSINESS_CONTROLLER_EMAIL_ADDRESS\n" +
                        ",CLAIMS_HANDLING_OPTION as CLAIMS_HANDLING_OPTION\n" +
                        ",CLAIMS_HANDLING_END_DATE as CLAIMS_HANDLING_END_DATE\n" +
                        ",BACKISSUES_HANDLING_OPTION as BACKISSUES_HANDLING_OPTION\n" +
                        ",BACKISSUES_HANDLING_END_DATE as BACKISSUES_HANDLING_END_DATE\n" +
                        ",NOTIFIED_DATE as NOTIFIED_DATE\n" +
                        "from " + Constants.JMF_SQL_SCHEMA + "." + table + "\n" +
                        " where PRODUCT_WORK_ID in ('%s')";
                break;

            case ("aws"):
                GET_DATA_FINANCE_INFO_JM = "select \n" +
                        "PRODUCT_WORK_ID as PRODUCT_WORK_ID\n" +
                        ",BUSINESS_CONTROLLER_NAME as BUSINESS_CONTROLLER_NAME\n" +
                        ",BUSINESS_UNIT_CODE as BUSINESS_UNIT_CODE\n" +
                        ",PMG_CODE as PMG_CODE\n" +
                        ",PMC_CODE as PMC_CODE\n" +
                        ",OPCO_R12_ID as OPCO_R12_ID\n" +
                        ",OPCO_R12_CODE as OPCO_R12_CODE\n" +
                        ",CONTRACT_OPCO_R12_CODE as CONTRACT_OPCO_R12_CODE\n" +
                        ",RESPONSIBILITY_CENTRE_CODE as RESPONSIBILITY_CENTRE_CODE\n" +
                        ",HFM_HIERARCHY_POSITION_CODE as HFM_HIERARCHY_POSITION_CODE\n" +
                        ",CITY_OPCO_R12_CODE as CITY_OPCO_R12_CODE\n" +
                        ",COUNTRY_OPCO_R12_CODE as COUNTRY_OPCO_R12_CODE\n" +
                        ",OPCO_R12_NAME as OPCO_R12_NAME\n" +
                        ",OPCO_R12_LEGAL_NAME as OPCO_R12_LEGAL_NAME\n" +
                        ",REFUND_OPTION as REFUND_OPTION\n" +
                        ",REMINDER_DATE as REMINDER_DATE\n" +
                        ",REMINDER_OPTION as REMINDER_OPTION\n" +
                        ",RENEWAL_OPTION as RENEWAL_OPTION\n" +
                        ",RENEWAL_DATE as RENEWAL_DATE\n" +
                        ",BUSINESS_CONTROLLER_EMAIL_ADDRESS as BUSINESS_CONTROLLER_EMAIL_ADDRESS\n" +
                        ",CLAIMS_HANDLING_OPTION as CLAIMS_HANDLING_OPTION\n" +
                        ",CLAIMS_HANDLING_END_DATE as CLAIMS_HANDLING_END_DATE\n" +
                        ",BACKISSUES_HANDLING_OPTION as BACKISSUES_HANDLING_OPTION\n" +
                        ",BACKISSUES_HANDLING_END_DATE as BACKISSUES_HANDLING_END_DATE\n" +
                        ",NOTIFIED_DATE as NOTIFIED_DATE\n" +
                        "from " + Constants.JMF_AWS_SCHEMA + "." + table + "\n" +
                        " where inbound_ts = (select inbound_ts from " + Constants.JMF_AWS_SCHEMA + "." + table + " order by inbound_ts desc limit 1)\n" +
                        " AND PRODUCT_WORK_ID in ('%s')";
                break;

        }
        return GET_DATA_FINANCE_INFO_JM;
    }

    public static String getLegalInfoSql(String serverEnv, String table) {
        String GET_DATA_LEGAL_INFO_JM = null;
        switch (serverEnv) {
            case ("mysql"):
                GET_DATA_LEGAL_INFO_JM = "select \n" +
                        "PRODUCT_WORK_ID as PRODUCT_WORK_ID\n" +
                        ",OWNERSHIP_BRAND_TYPE as OWNERSHIP_BRAND_TYPE\n" +
                        ",JOINT_OWNERSHIP_DETAILS as JOINT_OWNERSHIP_DETAILS\n" +
                        ",SOCIETY_CONTRACT_DATE as SOCIETY_CONTRACT_DATE\n" +
                        ",SOCIETY_CONTRACT_EXPIRY_DATE as SOCIETY_CONTRACT_EXPIRY_DATE\n" +
                        ",JOURNAL_COPYRIGHT_OWNER as JOURNAL_COPYRIGHT_OWNER\n" +
                        ",STANDARD_COPYRIGHT_STATEMENT_IND as STANDARD_COPYRIGHT_STATEMENT_IND\n" +
                        ",NON_STANDARD_COPYRIGHT_STATEMENT as NON_STANDARD_COPYRIGHT_STATEMENT\n" +
                        ",OPEN_ACCESS_ARTICLE_COPYRIGHT_TYPE as OPEN_ACCESS_ARTICLE_COPYRIGHT_TYPE\n" +
                        ",ARTICLE_DISCLAIMER as ARTICLE_DISCLAIMER\n" +
                        ",PUBLISHING_RIGHT_TYPE as PUBLISHING_RIGHT_TYPE\n" +
                        ",PRINT_RIGHTS_SECURED_IND as PRINT_RIGHTS_SECURED_IND\n" +
                        ",EXCLUSIVE_RIGHTS_SECURED_IND as EXCLUSIVE_RIGHTS_SECURED_IND\n" +
                        ",NO_EXCLUSIVE_PRINT_RIGHTS_REASON as NO_EXCLUSIVE_PRINT_RIGHTS_REASON\n" +
                        ",ELECTRONIC_PRINT_RIGHTS_SECURED_IND as ELECTRONIC_PRINT_RIGHTS_SECURED_IND\n" +
                        ",NO_EXCLUSIVE_ELECTRONIC_RIGHTS_REASON as NO_EXCLUSIVE_ELECTRONIC_RIGHTS_REASON\n" +
                        ",RIGHTS_RESTRICTIONS_IND as RIGHTS_RESTRICTIONS_IND\n" +
                        ",RIGHTS_RESTRICTIONS_LIST as RIGHTS_RESTRICTIONS_LIST\n" +
                        ",BACKFILE_CONSENT_IND as BACKFILE_CONSENT_IND\n" +
                        ",BACKFILE_CONSENT_INFO as BACKFILE_CONSENT_INFO\n" +
                        ",BACK_RIGHTS_TYPE as BACK_RIGHTS_TYPE\n" +
                        ",BACK_RIGHTS_START_VOLUME_AND_ISSUE as BACK_RIGHTS_START_VOLUME_AND_ISSUE\n" +
                        ",NON_EXCLUSIVE_ELECTRONIC_RIGHTS_IND as NON_EXCLUSIVE_ELECTRONIC_RIGHTS_IND\n" +
                        ",POST_TERMINATION_ELECTRONIC_RIGHTS_TYPE as POST_TERMINATION_ELECTRONIC_RIGHTS_TYPE\n" +
                        ",NO_POST_TERMINATION_ELECTRONIC_RIGHTS_REASON as NO_POST_TERMINATION_ELECTRONIC_RIGHTS_REASON\n" +
                        ",PERMISSION_TYPE as PERMISSION_TYPE\n" +
                        ",PERMISSION_HANDLING_EMAIL_ADDRESS as PERMISSION_HANDLING_EMAIL_ADDRESS\n" +
                        ",SOCIETY_MEMBERSHIP_HELD_TYPE as SOCIETY_MEMBERSHIP_HELD_TYPE\n" +
                        ",SPECIAL_ARRANGEMENTS as SPECIAL_ARRANGEMENTS\n" +
                        ",DESPATCH_METHOD as DESPATCH_METHOD\n" +
                        ",CONTRACT_SEEN_BY_ELS_ATTORNEY_IND as CONTRACT_SEEN_BY_ELS_ATTORNEY_IND\n" +
                        ",CONTRACT_SENT_IND as CONTRACT_SENT_IND\n" +
                        ",DIRECT_BILLING_MEMBERSHIP as DIRECT_BILLING_MEMBERSHIP\n" +
                        ",DIRECT_BILLING_MEMBERSHIP_WITH_FEE as DIRECT_BILLING_MEMBERSHIP_WITH_FEE\n" +
                        ",SOCIETY_MAINTAINED_PREPAYMENT as SOCIETY_MAINTAINED_PREPAYMENT\n" +
                        ",SOCIETY_MAINTAINED_ACCOUNT as SOCIETY_MAINTAINED_ACCOUNT\n" +
                        ",AUTOMATIC_RENEWAL as AUTOMATIC_RENEWAL\n" +
                        ",MAILING_LABELS as MAILING_LABELS\n" +
                        ",TITLE_INCLUDED_IN_CK as TITLE_INCLUDED_IN_CK\n" +
                        ",SOC_AGREED_TO_CK_CONTENT as SOC_AGREED_TO_CK_CONTENT\n" +
                        ",SOC_CK_CONTENT_AGREEMENT_TEXT as SOC_CK_CONTENT_AGREEMENT_TEXT\n" +
                        ",MONTHS_TO_KEEP_TRANSFER_ONLINE as MONTHS_TO_KEEP_TRANSFER_ONLINE\n" +
                        ",SOC_CK_CONTENT_AGREEMENT_DATE as SOC_CK_CONTENT_AGREEMENT_DATE\n" +
                        ",NOTIFIED_DATE as NOTIFIED_DATE\n" +
                        "from " + Constants.JMF_SQL_SCHEMA + "." + table + "\n" +
                        " where PRODUCT_WORK_ID in ('%s')";
                break;

            case ("aws"):
                GET_DATA_LEGAL_INFO_JM = "select \n" +
                        "PRODUCT_WORK_ID as PRODUCT_WORK_ID\n" +
                        ",OWNERSHIP_BRAND_TYPE as OWNERSHIP_BRAND_TYPE\n" +
                        ",JOINT_OWNERSHIP_DETAILS as JOINT_OWNERSHIP_DETAILS\n" +
                        ",SOCIETY_CONTRACT_DATE as SOCIETY_CONTRACT_DATE\n" +
                        ",SOCIETY_CONTRACT_EXPIRY_DATE as SOCIETY_CONTRACT_EXPIRY_DATE\n" +
                        ",JOURNAL_COPYRIGHT_OWNER as JOURNAL_COPYRIGHT_OWNER\n" +
                        ",STANDARD_COPYRIGHT_STATEMENT_IND as STANDARD_COPYRIGHT_STATEMENT_IND\n" +
                        ",NON_STANDARD_COPYRIGHT_STATEMENT as NON_STANDARD_COPYRIGHT_STATEMENT\n" +
                        ",OPEN_ACCESS_ARTICLE_COPYRIGHT_TYPE as OPEN_ACCESS_ARTICLE_COPYRIGHT_TYPE\n" +
                        ",ARTICLE_DISCLAIMER as ARTICLE_DISCLAIMER\n" +
                        ",PUBLISHING_RIGHT_TYPE as PUBLISHING_RIGHT_TYPE\n" +
                        ",PRINT_RIGHTS_SECURED_IND as PRINT_RIGHTS_SECURED_IND\n" +
                        ",EXCLUSIVE_RIGHTS_SECURED_IND as EXCLUSIVE_RIGHTS_SECURED_IND\n" +
                        ",NO_EXCLUSIVE_PRINT_RIGHTS_REASON as NO_EXCLUSIVE_PRINT_RIGHTS_REASON\n" +
                        ",ELECTRONIC_PRINT_RIGHTS_SECURED_IND as ELECTRONIC_PRINT_RIGHTS_SECURED_IND\n" +
                        ",NO_EXCLUSIVE_ELECTRONIC_RIGHTS_REASON as NO_EXCLUSIVE_ELECTRONIC_RIGHTS_REASON\n" +
                        ",RIGHTS_RESTRICTIONS_IND as RIGHTS_RESTRICTIONS_IND\n" +
                        ",RIGHTS_RESTRICTIONS_LIST as RIGHTS_RESTRICTIONS_LIST\n" +
                        ",BACKFILE_CONSENT_IND as BACKFILE_CONSENT_IND\n" +
                        ",BACKFILE_CONSENT_INFO as BACKFILE_CONSENT_INFO\n" +
                        ",BACK_RIGHTS_TYPE as BACK_RIGHTS_TYPE\n" +
                        ",BACK_RIGHTS_START_VOLUME_AND_ISSUE as BACK_RIGHTS_START_VOLUME_AND_ISSUE\n" +
                        ",NON_EXCLUSIVE_ELECTRONIC_RIGHTS_IND as NON_EXCLUSIVE_ELECTRONIC_RIGHTS_IND\n" +
                        ",POST_TERMINATION_ELECTRONIC_RIGHTS_TYPE as POST_TERMINATION_ELECTRONIC_RIGHTS_TYPE\n" +
                        ",NO_POST_TERMINATION_ELECTRONIC_RIGHTS_REASON as NO_POST_TERMINATION_ELECTRONIC_RIGHTS_REASON\n" +
                        ",PERMISSION_TYPE as PERMISSION_TYPE\n" +
                        ",PERMISSION_HANDLING_EMAIL_ADDRESS as PERMISSION_HANDLING_EMAIL_ADDRESS\n" +
                        ",SOCIETY_MEMBERSHIP_HELD_TYPE as SOCIETY_MEMBERSHIP_HELD_TYPE\n" +
                        ",SPECIAL_ARRANGEMENTS as SPECIAL_ARRANGEMENTS\n" +
                        ",DESPATCH_METHOD as DESPATCH_METHOD\n" +
                        ",CONTRACT_SEEN_BY_ELS_ATTORNEY_IND as CONTRACT_SEEN_BY_ELS_ATTORNEY_IND\n" +
                        ",CONTRACT_SENT_IND as CONTRACT_SENT_IND\n" +
                        ",DIRECT_BILLING_MEMBERSHIP as DIRECT_BILLING_MEMBERSHIP\n" +
                        ",DIRECT_BILLING_MEMBERSHIP_WITH_FEE as DIRECT_BILLING_MEMBERSHIP_WITH_FEE\n" +
                        ",SOCIETY_MAINTAINED_PREPAYMENT as SOCIETY_MAINTAINED_PREPAYMENT\n" +
                        ",SOCIETY_MAINTAINED_ACCOUNT as SOCIETY_MAINTAINED_ACCOUNT\n" +
                        ",AUTOMATIC_RENEWAL as AUTOMATIC_RENEWAL\n" +
                        ",MAILING_LABELS as MAILING_LABELS\n" +
                        ",TITLE_INCLUDED_IN_CK as TITLE_INCLUDED_IN_CK\n" +
                        ",SOC_AGREED_TO_CK_CONTENT as SOC_AGREED_TO_CK_CONTENT\n" +
                        ",SOC_CK_CONTENT_AGREEMENT_TEXT as SOC_CK_CONTENT_AGREEMENT_TEXT\n" +
                        ",MONTHS_TO_KEEP_TRANSFER_ONLINE as MONTHS_TO_KEEP_TRANSFER_ONLINE\n" +
                        ",SOC_CK_CONTENT_AGREEMENT_DATE as SOC_CK_CONTENT_AGREEMENT_DATE\n" +
                        ",NOTIFIED_DATE as NOTIFIED_DATE\n" +
                        "from " + Constants.JMF_AWS_SCHEMA + "." + table + "\n" +
                        " where inbound_ts = (select inbound_ts from " + Constants.JMF_AWS_SCHEMA + "." + table + " order by inbound_ts desc limit 1)\n" +
                        " AND PRODUCT_WORK_ID in ('%s')";
                break;

        }
        return GET_DATA_LEGAL_INFO_JM;
    }

    public static String getManifDetailSql(String serverEnv, String table) {
        String GET_DATA_MANIF_DETAIL_JM = null;
        switch (serverEnv) {
            case ("mysql"):
                GET_DATA_MANIF_DETAIL_JM = "select \n" +
                        "PRODUCT_MANIFESTATION_ID as PRODUCT_MANIFESTATION_ID\n" +
                        ",EMBARGO_TIMES_INDICATOR as EMBARGO_TIMES_INDICATOR\n" +
                        ",ELECTRONIC_RIGHTS_SECURED_TYPE as ELECTRONIC_RIGHTS_SECURED_TYPE\n" +
                        ",ONLINE_LAUNCH_DATE as ONLINE_LAUNCH_DATE\n" +
                        ",ARTICLE_IN_PRESS_S5_IND as ARTICLE_IN_PRESS_S5_IND\n" +
                        ",ARTICLE_IN_PRESS_S100_IND as ARTICLE_IN_PRESS_S100_IND\n" +
                        ",ARTICLE_IN_PRESS_S250_IND as ARTICLE_IN_PRESS_S250_IND\n" +
                        ",EMBARGO_TIMES_NUMBER as EMBARGO_TIMES_NUMBER\n" +
                        ",ONLINE_LAST_ISSUE_DATE as ONLINE_LAST_ISSUE_DATE\n" +
                        ",NOTIFIED_DATE as NOTIFIED_DATE\n" +
                        "from " + Constants.JMF_SQL_SCHEMA + "." + table + "\n" +
                        " where PRODUCT_MANIFESTATION_ID in ('%s')";
                break;
            case ("aws"):
                GET_DATA_MANIF_DETAIL_JM = "select \n" +
                        "PRODUCT_MANIFESTATION_ID as PRODUCT_MANIFESTATION_ID\n" +
                        ",EMBARGO_TIMES_INDICATOR as EMBARGO_TIMES_INDICATOR\n" +
                        ",ELECTRONIC_RIGHTS_SECURED_TYPE as ELECTRONIC_RIGHTS_SECURED_TYPE\n" +
                        ",ONLINE_LAUNCH_DATE as ONLINE_LAUNCH_DATE\n" +
                        ",ARTICLE_IN_PRESS_S5_IND as ARTICLE_IN_PRESS_S5_IND\n" +
                        ",ARTICLE_IN_PRESS_S100_IND as ARTICLE_IN_PRESS_S100_IND\n" +
                        ",ARTICLE_IN_PRESS_S250_IND as ARTICLE_IN_PRESS_S250_IND\n" +
                        ",EMBARGO_TIMES_NUMBER as EMBARGO_TIMES_NUMBER\n" +
                        ",ONLINE_LAST_ISSUE_DATE as ONLINE_LAST_ISSUE_DATE\n" +
                        ",NOTIFIED_DATE as NOTIFIED_DATE\n" +
                        "from " + Constants.JMF_AWS_SCHEMA + "." + table + "\n" +
                        " where inbound_ts = (select inbound_ts from " + Constants.JMF_AWS_SCHEMA + "." + table + " order by inbound_ts desc limit 1)\n" +
                        " AND PRODUCT_MANIFESTATION_ID in ('%s')";
                break;

        }
        return GET_DATA_MANIF_DETAIL_JM;

    }

    public static String getManifPrintSql(String serverEnv, String table) {
        String GET_DATA_MANIF_PRINT_JM = null;
        switch (serverEnv) {
            case ("mysql"):
                GET_DATA_MANIF_PRINT_JM = "select \n" +
                        "PRODUCT_MANIFESTATION_ID as PRODUCT_MANIFESTATION_ID\n" +
                        ",TRIM_SIZE as TRIM_SIZE\n" +
                        ",BASE_PRINT_RUN_NUMBER as BASE_PRINT_RUN_NUMBER\n" +
                        ",COLOUR_FREQUENCY as COLOUR_FREQUENCY\n" +
                        ",ARTWORK_SENSITIVITY_IND as ARTWORK_SENSITIVITY_IND\n" +
                        ",MAILING_BREAKDOWN_EUROPE as MAILING_BREAKDOWN_EUROPE\n" +
                        ",MAILING_BREAKDOWN_USA as MAILING_BREAKDOWN_USA\n" +
                        ",MAILING_BREAKDOWN_ROW as MAILING_BREAKDOWN_ROW\n" +
                        ",ZERO_WAREHOUSING_IND as ZERO_WAREHOUSING_IND\n" +
                        ",BACK_STOCK_WAREHOUSE_LOCATION_TYPE as BACK_STOCK_WAREHOUSE_LOCATION_TYPE\n" +
                        ",PRINTER_TYPE as PRINTER_TYPE\n" +
                        ",PRINTER_LOCATION_TYPE as PRINTER_LOCATION_TYPE\n" +
                        ",INTERIOR_PAPER_TYPE as INTERIOR_PAPER_TYPE\n" +
                        ",COVER_PAPER_TYPE as COVER_PAPER_TYPE\n" +
                        ",DISTRIBUTOR_CODE as DISTRIBUTOR_CODE\n" +
                        ",DISTRIBUTOR_LOCATION_CODE as DISTRIBUTOR_LOCATION_CODE\n" +
                        ",PRINT_MODEL_CODE as PRINT_MODEL_CODE\n" +
                        ",SEPARATE_PRINT_RUN_IND as SEPARATE_PRINT_RUN_IND\n" +
                        ",OFFPRINT_PRICING_CODE as OFFPRINT_PRICING_CODE\n" +
                        ",OFFPRINT_COVER_IND as OFFPRINT_COVER_IND\n" +
                        ",FREE_ISSUES_QUANTITY as FREE_ISSUES_QUANTITY\n" +
                        ",FREE_OFFPRINTS_TYPE as FREE_OFFPRINTS_TYPE\n" +
                        ",FREE_PAID_COLOUR_OFFPRINTS_QUANTITY as FREE_PAID_COLOUR_OFFPRINTS_QUANTITY\n" +
                        ",COLOUR_PRINTING_CURRENCY_CODE as COLOUR_PRINTING_CURRENCY_CODE\n" +
                        ",COLOUR_ARTWORK_EXCEPTIONS as COLOUR_ARTWORK_EXCEPTIONS\n" +
                        ",SOCIETY_OWNS_LABELS_IND as SOCIETY_OWNS_LABELS_IND\n" +
                        ",BINDING_TYPE as BINDING_TYPE\n" +
                        ",SPECIAL_BULK_ARRANGEMENTS as SPECIAL_BULK_ARRANGEMENTS\n" +
                        ",COST_FIRST_PRINTED_COLOUR_UNIT as COST_FIRST_PRINTED_COLOUR_UNIT\n" +
                        ",COST_NEXT_PRINTED_COLOUR_UNIT as COST_NEXT_PRINTED_COLOUR_UNIT\n" +
                        ",NOTIFIED_DATE as NOTIFIED_DATE\n" +
                        "from " + Constants.JMF_SQL_SCHEMA + "." + table + "\n" +
                        " where PRODUCT_MANIFESTATION_ID in ('%s')";
                break;
            case ("aws"):
                GET_DATA_MANIF_PRINT_JM = "select \n" +
                        "PRODUCT_MANIFESTATION_ID as PRODUCT_MANIFESTATION_ID\n" +
                        ",TRIM_SIZE as TRIM_SIZE\n" +
                        ",BASE_PRINT_RUN_NUMBER as BASE_PRINT_RUN_NUMBER\n" +
                        ",COLOUR_FREQUENCY as COLOUR_FREQUENCY\n" +
                        ",ARTWORK_SENSITIVITY_IND as ARTWORK_SENSITIVITY_IND\n" +
                        ",MAILING_BREAKDOWN_EUROPE as MAILING_BREAKDOWN_EUROPE\n" +
                        ",MAILING_BREAKDOWN_USA as MAILING_BREAKDOWN_USA\n" +
                        ",MAILING_BREAKDOWN_ROW as MAILING_BREAKDOWN_ROW\n" +
                        ",ZERO_WAREHOUSING_IND as ZERO_WAREHOUSING_IND\n" +
                        ",BACK_STOCK_WAREHOUSE_LOCATION_TYPE as BACK_STOCK_WAREHOUSE_LOCATION_TYPE\n" +
                        ",PRINTER_TYPE as PRINTER_TYPE\n" +
                        ",PRINTER_LOCATION_TYPE as PRINTER_LOCATION_TYPE\n" +
                        ",INTERIOR_PAPER_TYPE as INTERIOR_PAPER_TYPE\n" +
                        ",COVER_PAPER_TYPE as COVER_PAPER_TYPE\n" +
                        ",DISTRIBUTOR_CODE as DISTRIBUTOR_CODE\n" +
                        ",DISTRIBUTOR_LOCATION_CODE as DISTRIBUTOR_LOCATION_CODE\n" +
                        ",PRINT_MODEL_CODE as PRINT_MODEL_CODE\n" +
                        ",SEPARATE_PRINT_RUN_IND as SEPARATE_PRINT_RUN_IND\n" +
                        ",OFFPRINT_PRICING_CODE as OFFPRINT_PRICING_CODE\n" +
                        ",OFFPRINT_COVER_IND as OFFPRINT_COVER_IND\n" +
                        ",FREE_ISSUES_QUANTITY as FREE_ISSUES_QUANTITY\n" +
                        ",FREE_OFFPRINTS_TYPE as FREE_OFFPRINTS_TYPE\n" +
                        ",FREE_PAID_COLOUR_OFFPRINTS_QUANTITY as FREE_PAID_COLOUR_OFFPRINTS_QUANTITY\n" +
                        ",COLOUR_PRINTING_CURRENCY_CODE as COLOUR_PRINTING_CURRENCY_CODE\n" +
                        ",COLOUR_ARTWORK_EXCEPTIONS as COLOUR_ARTWORK_EXCEPTIONS\n" +
                        ",SOCIETY_OWNS_LABELS_IND as SOCIETY_OWNS_LABELS_IND\n" +
                        ",BINDING_TYPE as BINDING_TYPE\n" +
                        ",SPECIAL_BULK_ARRANGEMENTS as SPECIAL_BULK_ARRANGEMENTS\n" +
                        ",COST_FIRST_PRINTED_COLOUR_UNIT as COST_FIRST_PRINTED_COLOUR_UNIT\n" +
                        ",COST_NEXT_PRINTED_COLOUR_UNIT as COST_NEXT_PRINTED_COLOUR_UNIT\n" +
                        ",NOTIFIED_DATE as NOTIFIED_DATE\n" +
                        "from " + Constants.JMF_AWS_SCHEMA + "." + table + "\n" +
                        " where inbound_ts = (select inbound_ts from " + Constants.JMF_AWS_SCHEMA + "." + table + " order by inbound_ts desc limit 1)\n" +
                        " AND PRODUCT_MANIFESTATION_ID in ('%s')";
                break;

        }
        return GET_DATA_MANIF_PRINT_JM;

    }

    public static String getPartyProdSql(String serverEnv, String table) {
        String GET_DATA_PARTY_PROD_JM = null;
        switch (serverEnv) {
            case ("mysql"):
                GET_DATA_PARTY_PROD_JM = "select \n" +
                        "PARTY_IN_PRODUCT_ID as PARTY_IN_PRODUCT_ID\n" +
                        ",F_PRODUCT_WORK as F_PRODUCT_WORK\n" +
                        ",PARTY_ROLE_TYPE as PARTY_ROLE_TYPE\n" +
                        ",EMAIL_ADDRESS as EMAIL_ADDRESS\n" +
                        ",FULL_NAME as FULL_NAME\n" +
                        ",PHONE_NUMBER as PHONE_NUMBER\n" +
                        ",ADDRESS_LINE_1 as ADDRESS_LINE_1\n" +
                        ",ADDRESS_LINE_2 as ADDRESS_LINE_2\n" +
                        ",ADDRESS_LINE_3 as ADDRESS_LINE_3\n" +
                        ",CITY as CITY\n" +
                        ",COUNTRY as COUNTRY\n" +
                        ",STATE as STATE\n" +
                        ",POST_CODE as POST_CODE\n" +
                        ",ORGANISATION_1 as ORGANISATION_1\n" +
                        ",PMX_PARTY_ID as PMX_PARTY_ID\n" +
                        ",PEOPLEHUB_ID as PEOPLEHUB_ID\n" +
                        ",NOTIFIED_DATE as NOTIFIED_DATE\n" +
                        ",EPH_PERSON_ID as EPH_PERSON_ID\n" +
                        "from " + Constants.JMF_SQL_SCHEMA + "." + table + "\n" +
                        " where PARTY_IN_PRODUCT_ID in ('%s')";
                break;
            case ("aws"):
                GET_DATA_PARTY_PROD_JM = "select \n" +
                        "PARTY_IN_PRODUCT_ID as PARTY_IN_PRODUCT_ID\n" +
                        ",F_PRODUCT_WORK as F_PRODUCT_WORK\n" +
                        ",PARTY_ROLE_TYPE as PARTY_ROLE_TYPE\n" +
                        ",EMAIL_ADDRESS as EMAIL_ADDRESS\n" +
                        ",FULL_NAME as FULL_NAME\n" +
                        ",PHONE_NUMBER as PHONE_NUMBER\n" +
                        ",ADDRESS_LINE_1 as ADDRESS_LINE_1\n" +
                        ",ADDRESS_LINE_2 as ADDRESS_LINE_2\n" +
                        ",ADDRESS_LINE_3 as ADDRESS_LINE_3\n" +
                        ",CITY as CITY\n" +
                        ",COUNTRY as COUNTRY\n" +
                        ",STATE as STATE\n" +
                        ",POST_CODE as POST_CODE\n" +
                        ",ORGANISATION_1 as ORGANISATION_1\n" +
                        ",PMX_PARTY_ID as PMX_PARTY_ID\n" +
                        ",PEOPLEHUB_ID as PEOPLEHUB_ID\n" +
                        ",NOTIFIED_DATE as NOTIFIED_DATE\n" +
                        ",EPH_PERSON_ID as EPH_PERSON_ID\n" +
                        "from " + Constants.JMF_AWS_SCHEMA + "." + table + "\n" +
                        " where inbound_ts = (select inbound_ts from " + Constants.JMF_AWS_SCHEMA + "." + table + " order by inbound_ts desc limit 1)\n" +
                        " AND PARTY_IN_PRODUCT_ID in ('%s')";
                break;

        }
        return GET_DATA_PARTY_PROD_JM;

    }

    public static String getProdAvailSql(String serverEnv, String table) {
        String GET_DATA_PRODUCT_AVAIL_JM = null;
        switch (serverEnv) {
            case ("mysql"):
                GET_DATA_PRODUCT_AVAIL_JM = "select \n" +
                        "PRODUCT_AVAILABILITY_ID as PRODUCT_AVAILABILITY_ID\n" +
                        ",F_PRODUCT_MANIFESTATION as F_PRODUCT_MANIFESTATION\n" +
                        ",APPLICATION_CODE as APPLICATION_CODE\n" +
                        ",NOTIFIED_DATE as NOTIFIED_DATE\n" +
                        "from " + Constants.JMF_SQL_SCHEMA + "." + table + "\n" +
                        " where PRODUCT_AVAILABILITY_ID in ('%s')";
                break;
            case ("aws"):
                GET_DATA_PRODUCT_AVAIL_JM = "select \n" +
                        "PRODUCT_AVAILABILITY_ID as PRODUCT_AVAILABILITY_ID\n" +
                        ",F_PRODUCT_MANIFESTATION as F_PRODUCT_MANIFESTATION\n" +
                        ",APPLICATION_CODE as APPLICATION_CODE\n" +
                        ",NOTIFIED_DATE as NOTIFIED_DATE\n" +
                        "from " + Constants.JMF_AWS_SCHEMA + "." + table + "\n" +
                        " where inbound_ts = (select inbound_ts from " + Constants.JMF_AWS_SCHEMA + "." + table + " order by inbound_ts desc limit 1)\n" +
                        " AND PRODUCT_AVAILABILITY_ID in ('%s')";
                break;

        }
        return GET_DATA_PRODUCT_AVAIL_JM;

    }

}




