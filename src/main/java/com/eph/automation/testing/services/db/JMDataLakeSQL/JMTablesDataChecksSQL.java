package com.eph.automation.testing.services.db.JMDataLakeSQL;

import static com.eph.automation.testing.services.db.JMDataLakeSQL.GetJMDLDBUser.getJMDataBase;


public class JMTablesDataChecksSQL {

    static  String[] databaseEnv = getJMDataBase();

    public static String GET_ALLOCATION_IDS = "select ALLOCATION_CHANGE_ID from " + databaseEnv[0] + ".JMF_ALLOCATION_CHANGE order by rand() limit %s";

    public static String GET_APPLICATION_KEY = "select APPLICATION_PROPERTY_KEY from " + databaseEnv[0] + ".JMF_APPLICATION_PROPERTIES order by rand() limit %s";

    public static String GET_APPROVAL_REQ_ID = "select APPROVAL_ID from " + databaseEnv[0] + ".JMF_APPROVAL_REQUEST order by rand() limit %s";

    public static String GET_CHRONICLE_SCE_ID = "select CHRONICLE_SCENARIO_CODE from " + databaseEnv[0] + ".JMF_CHRONICLE_SCENARIO order by rand() limit %s";

    public static String GET_CHRONICLE_STATUS_ID = "select CHRONICLE_STATUS_CODE from " + databaseEnv[0] + ".JMF_CHRONICLE_STATUS order by rand() limit %s";

    public static String GET_FAMILY_RESOURCE_ID = "select FAMILY_RESOURCE_DETAILS_ID from " + databaseEnv[0] + ".JMF_FAMILY_RESOURCE_DETAILS order by rand() limit %s";

    public static String GET_PRODUCT_WORK_ID = "select PRODUCT_WORK_ID from " + databaseEnv[0] + ".JMF_FINANCIAL_INFORMATION order by rand() limit %s";

    public static String GET_LEGAL_PRODUCT_WORK_ID = "select PRODUCT_WORK_ID from " + databaseEnv[0] + ".JMF_LEGAL_INFORMATION order by rand() limit %s";

    public static String GET_MANIF_PRODUCT_ID = "select PRODUCT_MANIFESTATION_ID from " + databaseEnv[0] + ".JMF_MANIFESTATION_ELECTRONIC_DETAILS order by rand() limit %s";

    public static String GET_MANIF_PRINT_ID = "select PRODUCT_MANIFESTATION_ID from " + databaseEnv[0] + ".JMF_MANIFESTATION_PRINT_DETAILS order by rand() limit %s";

    public static String GET_PARTY_PROD_ID = "select PARTY_IN_PRODUCT_ID from " + databaseEnv[0] + ".JMF_PARTY_IN_PRODUCT order by rand() limit %s";

    public static String GET_PRODUCT_AVAIL_ID = "select PRODUCT_AVAILABILITY_ID from " + databaseEnv[0] + ".JMF_PRODUCT_AVAILABILITY order by rand() limit %s";

    public static String GET_PRODUCT_CHRONICLE_ID = "select PRODUCT_CHRONICLE_ID from " + databaseEnv[0] + ".JMF_PRODUCT_CHRONICLE order by rand() limit %s";

    public static String GET_PRODUCT_FAMILY_ID = "select PRODUCT_FAMILY_ID from " + databaseEnv[0] + ".JMF_PRODUCT_FAMILY order by rand() limit %s";

    public static String GET_PRODUCT_MANIF_ID = "select PRODUCT_MANIFESTATION_ID from " + databaseEnv[0] + ".JMF_PRODUCT_MANIFESTATION order by rand() limit %s";

    public static String GET_PRODUCT_SUBJ_ID = "select PRODUCT_SUBJECT_AREA_ID from " + databaseEnv[0] + ".JMF_PRODUCT_SUBJECT_AREA order by rand() limit %s";

    public static String GET_REVIEW_COMMENTS_ID = "select REVIEW_COMMENT_ID from " + databaseEnv[0] + ".JMF_REVIEW_COMMENT order by rand() limit %s";

    public static String GET_PROD_WORK_ID = "select PRODUCT_WORK_ID from " + databaseEnv[0] + ".JMF_PRODUCT_WORK order by rand() limit %s";

    public static String GET_APPROVAL_ID = "select APPROVAL_ATTACHMENT_ID from " + databaseEnv[0] + ".JMF_APPROVAL_ATTACHMENT order by rand() limit %s";

    public static String GET_PRODUCTION_INFORMATION_ID = "select PRODUCT_WORK_ID from " + databaseEnv[0] + ".JMF_PRODUCTION_INFORMATION order by rand() limit %s";

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
                        "from " + databaseEnv[0] + "." + table + "\n" +
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
                        "from " + databaseEnv[1] + "." + table + "\n" +
                        " where inbound_ts = (select inbound_ts from " + databaseEnv[1] + "." + table + " order by inbound_ts desc limit 1)\n" +
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
                        "from " + databaseEnv[0] + "." + table + "\n" +
                        " where APPLICATION_PROPERTY_KEY in ('%s')";
                break;
            case ("aws"):
                GET_DATA_APP_PROP_JM = "select \n" +
                        "APPLICATION_PROPERTY_KEY as APPLICATION_PROPERTY_KEY\n" +
                        ",APPLICATION_PROPERTY_VALUE as APPLICATION_PROPERTY_VALUE\n" +
                        ",NOTIFIED_DATE as NOTIFIED_DATE\n" +
                        "from " + databaseEnv[1] + "." + table + "\n" +
                        " where inbound_ts = (select inbound_ts from " + databaseEnv[1] + "." + table + " order by inbound_ts desc limit 1)\n" +
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
                        "from " + databaseEnv[0] + "." + table + "\n" +
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
                        "from " + databaseEnv[1] + "." + table + "\n" +
                        " where inbound_ts = (select inbound_ts from " + databaseEnv[1] + "." + table + " order by inbound_ts desc limit 1)\n" +
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
                        "from " + databaseEnv[0] + "." + table + "\n" +
                        " where CHRONICLE_SCENARIO_CODE in ('%s')";
                break;
            case ("aws"):
                GET_DATA_CHRO_SCE_JM = "select \n" +
                        "CHRONICLE_SCENARIO_CODE as CHRONICLE_SCENARIO_CODE\n" +
                        ",CHRONICLE_SCENARIO_NAME as CHRONICLE_SCENARIO_NAME\n" +
                        ",CHRONICLE_SCENARIO_DESCRIPTION as CHRONICLE_SCENARIO_DESCRIPTION\n" +
                        ",CHRONICLE_SCENARIO_EVOLUTIONARY_IND as CHRONICLE_SCENARIO_EVOLUTIONARY_IND\n" +
                        ",NOTIFIED_DATE as NOTIFIED_DATE\n" +
                        "from " + databaseEnv[1] + "." + table + "\n" +
                        " where inbound_ts = (select inbound_ts from " + databaseEnv[1] + "." + table + " order by inbound_ts desc limit 1)\n" +
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
                        "from " + databaseEnv[0] + "." + table + "\n" +
                        " where CHRONICLE_STATUS_CODE in ('%s')";
                break;
            case ("aws"):
                GET_DATA_CHRO_STATUS_JM = "select \n" +
                        "CHRONICLE_STATUS_CODE as CHRONICLE_STATUS_CODE\n" +
                        ",CHRONICLE_STATUS_NAME as CHRONICLE_STATUS_NAME\n" +
                        ",CHRONICLE_STATUS_DESCRIPTION as CHRONICLE_STATUS_DESCRIPTION\n" +
                        ",NOTIFIED_DATE as NOTIFIED_DATE\n" +
                        "from " + databaseEnv[1] + "." + table + "\n" +
                        " where inbound_ts = (select inbound_ts from " + databaseEnv[1] + "." + table + " order by inbound_ts desc limit 1)\n" +
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
                        "from " + databaseEnv[0] + "." + table + "\n" +
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
                        "from " + databaseEnv[1] + "." + table + "\n" +
                        " where inbound_ts = (select inbound_ts from " + databaseEnv[1] + "." + table + " order by inbound_ts desc limit 1)\n" +
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
                        "from " + databaseEnv[0] + "." + table + "\n" +
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
                        "from " + databaseEnv[1] + "." + table + "\n" +
                        " where inbound_ts = (select inbound_ts from " + databaseEnv[1] + "." + table + " order by inbound_ts desc limit 1)\n" +
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
                        "from " + databaseEnv[0] + "." + table + "\n" +
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
                        "from " + databaseEnv[1] + "." + table + "\n" +
                        " where inbound_ts = (select inbound_ts from " + databaseEnv[1] + "." + table + " order by inbound_ts desc limit 1)\n" +
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
                        "from " + databaseEnv[0] + "." + table + "\n" +
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
                        "from " + databaseEnv[1] + "." + table + "\n" +
                        " where inbound_ts = (select inbound_ts from " + databaseEnv[1] + "." + table + " order by inbound_ts desc limit 1)\n" +
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
                        "from " + databaseEnv[0] + "." + table + "\n" +
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
                        "from " + databaseEnv[1] + "." + table + "\n" +
                        " where inbound_ts = (select inbound_ts from " + databaseEnv[1] + "." + table + " order by inbound_ts desc limit 1)\n" +
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
                        "from " + databaseEnv[0] + "." + table + "\n" +
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
                        "from " + databaseEnv[1] + "." + table + "\n" +
                        " where inbound_ts = (select inbound_ts from " + databaseEnv[1] + "." + table + " order by inbound_ts desc limit 1)\n" +
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
                        "from " + databaseEnv[0] + "." + table + "\n" +
                        " where PRODUCT_AVAILABILITY_ID in ('%s')";
                break;
            case ("aws"):
                GET_DATA_PRODUCT_AVAIL_JM = "select \n" +
                        "PRODUCT_AVAILABILITY_ID as PRODUCT_AVAILABILITY_ID\n" +
                        ",F_PRODUCT_MANIFESTATION as F_PRODUCT_MANIFESTATION\n" +
                        ",APPLICATION_CODE as APPLICATION_CODE\n" +
                        ",NOTIFIED_DATE as NOTIFIED_DATE\n" +
                        "from " + databaseEnv[1] + "." + table + "\n" +
                        " where inbound_ts = (select inbound_ts from " + databaseEnv[1] + "." + table + " order by inbound_ts desc limit 1)\n" +
                        " AND PRODUCT_AVAILABILITY_ID in ('%s')";
                break;
        }
        return GET_DATA_PRODUCT_AVAIL_JM;
    }

    public static String getPrdtChronicleSql(String serverEnv, String table) {
        String GET_DATA_PRODUCT_CHRONICLE_JM = null;
        switch (serverEnv) {
            case ("mysql"):
                GET_DATA_PRODUCT_CHRONICLE_JM = "select \n" +
                        "PRODUCT_CHRONICLE_ID as PRODUCT_CHRONICLE_ID\n" +
                        ",COMPLETED_ON as COMPLETED_ON\n" +
                        ",DISTRIBUTION_LIST as DISTRIBUTION_LIST\n" +
                        ",RENAME_REQUIRED_IND as RENAME_REQUIRED_IND\n" +
                        ",CHRONICLE_STATUS_CODE as CHRONICLE_STATUS_CODE\n" +
                        ",CHRONICLE_SCENARIO_CODE as CHRONICLE_SCENARIO_CODE\n" +
                        ",STARTED_BY as STARTED_BY\n" +
                        ",STARTED_ON as STARTED_ON\n" +
                        ",UPDATED_BY as UPDATED_BY\n" +
                        ",UPDATED_ON as UPDATED_ON\n" +
                        ",PROCESS_INSTANCE_ID as PROCESS_INSTANCE_ID\n" +
                        ",REASON_FOR_CHANGE as REASON_FOR_CHANGE\n" +
                        ",CANCELLED_BY as CANCELLED_BY\n" +
                        ",CREATED_BY_NAME as CREATED_BY_NAME\n" +
                        ",REJECTION_COMMENT as REJECTION_COMMENT\n" +
                        ",SUBMISSION_DATE as SUBMISSION_DATE\n" +
                        ",CANCELLED_DATE as CANCELLED_DATE\n" +
                        ",REJECTION_DATE as REJECTION_DATE\n" +
                        ",VERSION as VERSION\n" +
                        ",NOTIFIED_DATE as NOTIFIED_DATE\n" +
                        "from " + databaseEnv[0] + "." + table + "\n" +
                        " where PRODUCT_CHRONICLE_ID in ('%s')";
                break;
            case ("aws"):
                GET_DATA_PRODUCT_CHRONICLE_JM = "select \n" +
                        "PRODUCT_CHRONICLE_ID as PRODUCT_CHRONICLE_ID\n" +
                        ",COMPLETED_ON as COMPLETED_ON\n" +
                        ",DISTRIBUTION_LIST as DISTRIBUTION_LIST\n" +
                        ",RENAME_REQUIRED_IND as RENAME_REQUIRED_IND\n" +
                        ",CHRONICLE_STATUS_CODE as CHRONICLE_STATUS_CODE\n" +
                        ",CHRONICLE_SCENARIO_CODE as CHRONICLE_SCENARIO_CODE\n" +
                        ",STARTED_BY as STARTED_BY\n" +
                        ",STARTED_ON as STARTED_ON\n" +
                        ",UPDATED_BY as UPDATED_BY\n" +
                        ",UPDATED_ON as UPDATED_ON\n" +
                        ",PROCESS_INSTANCE_ID as PROCESS_INSTANCE_ID\n" +
                        ",REASON_FOR_CHANGE as REASON_FOR_CHANGE\n" +
                        ",CANCELLED_BY as CANCELLED_BY\n" +
                        ",CREATED_BY_NAME as CREATED_BY_NAME\n" +
                        ",REJECTION_COMMENT as REJECTION_COMMENT\n" +
                        ",SUBMISSION_DATE as SUBMISSION_DATE\n" +
                        ",CANCELLED_DATE as CANCELLED_DATE\n" +
                        ",REJECTION_DATE as REJECTION_DATE\n" +
                        ",VERSION as VERSION\n" +
                        ",NOTIFIED_DATE as NOTIFIED_DATE\n" +
                        "from " + databaseEnv[1] + "." + table + "\n" +
                        " where inbound_ts = (select inbound_ts from " + databaseEnv[1] + "." + table + " order by inbound_ts desc limit 1)\n" +
                        " AND PRODUCT_CHRONICLE_ID in ('%s')";
                break;
        }
        return GET_DATA_PRODUCT_CHRONICLE_JM;

    }

    public static String getProdFamilySql(String serverEnv, String table) {
        String GET_DATA_PRODUCT_FAMILY_JM = null;
        switch (serverEnv) {
            case ("mysql"):
                GET_DATA_PRODUCT_FAMILY_JM = "select \n" +
                        "PRODUCT_FAMILY_ID as PRODUCT_FAMILY_ID\n" +
                        ",F_PRODUCT_CHRONICLE as F_PRODUCT_CHRONICLE\n" +
                        ",PRODUCT_FAMILY_TITLE as PRODUCT_FAMILY_TITLE\n" +
                        ",PRODUCT_JOURNEY_IDENTIFIER as PRODUCT_JOURNEY_IDENTIFIER\n" +
                        ",PMX_PRODUCT_FAMILY_ID as PMX_PRODUCT_FAMILY_ID\n" +
                        ",NOTIFIED_DATE as NOTIFIED_DATE\n" +
                        "from " + databaseEnv[0] + "." + table + "\n" +
                        " where PRODUCT_FAMILY_ID in ('%s')";
                break;
            case ("aws"):
                GET_DATA_PRODUCT_FAMILY_JM = "select \n" +
                        "PRODUCT_FAMILY_ID as PRODUCT_FAMILY_ID\n" +
                        ",F_PRODUCT_CHRONICLE as F_PRODUCT_CHRONICLE\n" +
                        ",PRODUCT_FAMILY_TITLE as PRODUCT_FAMILY_TITLE\n" +
                        ",PRODUCT_JOURNEY_IDENTIFIER as PRODUCT_JOURNEY_IDENTIFIER\n" +
                        ",PMX_PRODUCT_FAMILY_ID as PMX_PRODUCT_FAMILY_ID\n" +
                        ",NOTIFIED_DATE as NOTIFIED_DATE\n" +
                        "from " + databaseEnv[1] + "." + table + "\n" +
                        " where inbound_ts = (select inbound_ts from " + databaseEnv[1] + "." + table + " order by inbound_ts desc limit 1)\n" +
                        " AND PRODUCT_FAMILY_ID in ('%s')";
                break;
        }
        return GET_DATA_PRODUCT_FAMILY_JM;
    }

    public static String getProdManifSql(String serverEnv, String table) {
        String GET_DATA_PRODUCT_MANIF_JM = null;
        switch (serverEnv) {
            case ("mysql"):
                GET_DATA_PRODUCT_MANIF_JM ="select \n" +
                        "PRODUCT_MANIFESTATION_ID as PRODUCT_MANIFESTATION_ID\n" +
                        ",F_PRODUCT_WORK as F_PRODUCT_WORK\n" +
                        ",PRODUCT_MANIFESTATION_TITLE as PRODUCT_MANIFESTATION_TITLE\n" +
                        ",ISSN as ISSN\n" +
                        ",ELSEVIER_JOURNAL_NUMBER as ELSEVIER_JOURNAL_NUMBER\n" +
                        ",SUBSCRIPTION_TYPE as SUBSCRIPTION_TYPE\n" +
                        ",PRICE_CATEGORIES as PRICE_CATEGORIES\n" +
                        ",PMX_PRODUCT_MANIFESTATION_ID as PMX_PRODUCT_MANIFESTATION_ID\n" +
                        ",NOTIFIED_DATE as NOTIFIED_DATE\n" +
                        ",EPH_MANIFESTATION_ID as EPH_MANIFESTATION_ID\n" +
                        "from " + databaseEnv[0] + "." + table + "\n" +
                        " where PRODUCT_MANIFESTATION_ID in ('%s')";
                break;
            case ("aws"):
                GET_DATA_PRODUCT_MANIF_JM = "select \n" +
                        "PRODUCT_MANIFESTATION_ID as PRODUCT_MANIFESTATION_ID\n" +
                        ",F_PRODUCT_WORK as F_PRODUCT_WORK\n" +
                        ",PRODUCT_MANIFESTATION_TITLE as PRODUCT_MANIFESTATION_TITLE\n" +
                        ",ISSN as ISSN\n" +
                        ",ELSEVIER_JOURNAL_NUMBER as ELSEVIER_JOURNAL_NUMBER\n" +
                        ",SUBSCRIPTION_TYPE as SUBSCRIPTION_TYPE\n" +
                        ",PRICE_CATEGORIES as PRICE_CATEGORIES\n" +
                        ",PMX_PRODUCT_MANIFESTATION_ID as PMX_PRODUCT_MANIFESTATION_ID\n" +
                        ",NOTIFIED_DATE as NOTIFIED_DATE\n" +
                        ",EPH_MANIFESTATION_ID as EPH_MANIFESTATION_ID\n" +
                        "from " + databaseEnv[1] + "." + table + "\n" +
                        " where inbound_ts = (select inbound_ts from " + databaseEnv[1] + "." + table + " order by inbound_ts desc limit 1)\n" +
                        " AND PRODUCT_MANIFESTATION_ID in ('%s')";
                break;
        }
        return GET_DATA_PRODUCT_MANIF_JM;
    }

    public static String getProdSubjSql(String serverEnv, String table) {
        String GET_DATA_PROD_SUBJ_JM = null;
        switch (serverEnv) {
            case ("mysql"):
                GET_DATA_PROD_SUBJ_JM = "select \n" +
                        "PRODUCT_SUBJECT_AREA_ID as PRODUCT_SUBJECT_AREA_ID\n" +
                        ",F_PRODUCT_WORK as F_PRODUCT_WORK\n" +
                        ",SUBJECT_AREA_TYPE_CODE as SUBJECT_AREA_TYPE_CODE\n" +
                        ",SUBJECT_AREA_PRIORITY_CODE as SUBJECT_AREA_PRIORITY_CODE\n" +
                        ",SUBJECT_AREA_CODE as SUBJECT_AREA_CODE\n" +
                        ",SUBJECT_AREA_NAME as SUBJECT_AREA_NAME\n" +
                        ",NOTIFIED_DATE as NOTIFIED_DATE\n" +
                        "from " + databaseEnv[0] + "." + table + "\n" +
                        " where PRODUCT_SUBJECT_AREA_ID in ('%s')";
                break;
            case ("aws"):
                GET_DATA_PROD_SUBJ_JM = "select \n" +
                        "PRODUCT_SUBJECT_AREA_ID as PRODUCT_SUBJECT_AREA_ID\n" +
                        ",F_PRODUCT_WORK as F_PRODUCT_WORK\n" +
                        ",SUBJECT_AREA_TYPE_CODE as SUBJECT_AREA_TYPE_CODE\n" +
                        ",SUBJECT_AREA_PRIORITY_CODE as SUBJECT_AREA_PRIORITY_CODE\n" +
                        ",SUBJECT_AREA_CODE as SUBJECT_AREA_CODE\n" +
                        ",SUBJECT_AREA_NAME as SUBJECT_AREA_NAME\n" +
                        ",NOTIFIED_DATE as NOTIFIED_DATE\n" +
                        "from " + databaseEnv[1] + "." + table + "\n" +
                        " where inbound_ts = (select inbound_ts from " + databaseEnv[1] + "." + table + " order by inbound_ts desc limit 1)\n" +
                        " AND PRODUCT_SUBJECT_AREA_ID in ('%s')";
                break;

        }
        return GET_DATA_PROD_SUBJ_JM;

    }


    public static String getReviewCommentsSql(String serverEnv, String table) {
        String GET_DATA_REVIEW_COMMENT_JM = null;
        switch (serverEnv) {
            case ("mysql"):
                GET_DATA_REVIEW_COMMENT_JM = "select \n" +
                        "REVIEW_COMMENT_ID as REVIEW_COMMENT_ID\n" +
                        ",F_APPROVAL_ID as F_APPROVAL_ID\n" +
                        ",REVIEW_ATTRIBUTE_NAME as REVIEW_ATTRIBUTE_NAME\n" +
                        ",REVIEW_COMMENT as REVIEW_COMMENT\n" +
                        ",REVIEW_COMMENT_DATE as REVIEW_COMMENT_DATE\n" +
                        ",CREATED_ON as CREATED_ON\n" +
                        ",NOTIFIED_DATE as NOTIFIED_DATE\n" +
                        "from " + databaseEnv[0] + "." + table + "\n" +
                        " where REVIEW_COMMENT_ID in ('%s')";
                break;
            case ("aws"):
                GET_DATA_REVIEW_COMMENT_JM = "select \n" +
                        "REVIEW_COMMENT_ID as REVIEW_COMMENT_ID\n" +
                        ",F_APPROVAL_ID as F_APPROVAL_ID\n" +
                        ",REVIEW_ATTRIBUTE_NAME as REVIEW_ATTRIBUTE_NAME\n" +
                        ",REVIEW_COMMENT as REVIEW_COMMENT\n" +
                        ",REVIEW_COMMENT_DATE as REVIEW_COMMENT_DATE\n" +
                        ",CREATED_ON as CREATED_ON\n" +
                        ",NOTIFIED_DATE as NOTIFIED_DATE\n" +
                        "from " + databaseEnv[1] + "." + table + "\n" +
                        " where inbound_ts = (select inbound_ts from " + databaseEnv[1] + "." + table + " order by inbound_ts desc limit 1)\n" +
                        " AND REVIEW_COMMENT_ID in ('%s')";
                break;

        }
        return GET_DATA_REVIEW_COMMENT_JM;
    }

    public static String getProdWorkSql(String serverEnv, String table) {
        String GET_DATA_PRODUCT_WORK_JM = null;
        switch (serverEnv) {
            case ("mysql"):
                GET_DATA_PRODUCT_WORK_JM = "select \n" +
                        "PRODUCT_WORK_ID as PRODUCT_WORK_ID\n" +
                        ",F_PRODUCT_FAMILY as F_PRODUCT_FAMILY\n" +
                        ",PRODUCT_WORK_TITLE as PRODUCT_WORK_TITLE\n" +
                        ",PRODUCT_SUBTITLE as PRODUCT_SUBTITLE\n" +
                        ",PRODUCT_WORK_TITLE_INFO as PRODUCT_WORK_TITLE_INFO\n" +
                        ",ISSN_L as ISSN_L\n" +
                        ",ELSEVIER_JOURNAL_NUMBER as ELSEVIER_JOURNAL_NUMBER\n" +
                        ",INTERNAL_ELSEVIER_DIVISION as INTERNAL_ELSEVIER_DIVISION\n" +
                        ",MANIFESTATION_TYPES_CODE as MANIFESTATION_TYPES_CODE\n" +
                        ",MANIFESTATION_PERSONAL_MODEL_TYPE as MANIFESTATION_PERSONAL_MODEL_TYPE\n" +
                        ",IMPRINT_NAME as IMPRINT_NAME\n" +
                        ",PTS_JOURNAL_INDICATOR as PTS_JOURNAL_INDICATOR\n" +
                        ",YEAR_OF_FIRST_ISSUE as YEAR_OF_FIRST_ISSUE\n" +
                        ",YEAR_OF_LAST_ISSUE as YEAR_OF_LAST_ISSUE\n" +
                        ",FIRST_VOLUME_NAME as FIRST_VOLUME_NAME\n" +
                        ",FIRST_ISSUE_NAME as FIRST_ISSUE_NAME\n" +
                        ",LAST_VOLUME_NAME as LAST_VOLUME_NAME\n" +
                        ",LAST_ISSUE_NAME as LAST_ISSUE_NAME\n" +
                        ",VOLUMES_PER_YEAR_QUANTITY as VOLUMES_PER_YEAR_QUANTITY\n" +
                        ",ISSUES_PER_VOLUME_QUANTITY as ISSUES_PER_VOLUME_QUANTITY\n" +
                        ",FIRST_YEAR_VOLUMES_PER_YEAR_QUANTITY as FIRST_YEAR_VOLUMES_PER_YEAR_QUANTITY\n" +
                        ",FIRST_YEAR_ISSUES_PER_VOLUME_QUANTITY as FIRST_YEAR_ISSUES_PER_VOLUME_QUANTITY\n" +
                        ",PERIODICAL_TIMING_DESC as PERIODICAL_TIMING_DESC\n" +
                        ",OPEN_ACCESSTYPE_CODE as OPEN_ACCESSTYPE_CODE\n" +
                        ",OPEN_ACCESS_SPONSOR_NAME as OPEN_ACCESS_SPONSOR_NAME\n" +
                        ",OPEN_ACCESS_FEE as OPEN_ACCESS_FEE\n" +
                        ",OPEN_ACCESS_CURRENCY_CODE as OPEN_ACCESS_CURRENCY_CODE\n" +
                        ",OPEN_ACCESS_DISCOUNT_IND as OPEN_ACCESS_DISCOUNT_IND\n" +
                        ",OPEN_ACCESS_DISCOUNT_PERIOD as OPEN_ACCESS_DISCOUNT_PERIOD\n" +
                        ",OA_FIRST_YEAR_DISCOUNT_PRICE as OA_FIRST_YEAR_DISCOUNT_PRICE\n" +
                        ",OA_SECOND_YEAR_DISCOUNT_PRICE as OA_SECOND_YEAR_DISCOUNT_PRICE\n" +
                        ",DDP_ELIGIBLE_IND as DDP_ELIGIBLE_IND\n" +
                        ",WEBSHOP_IND as WEBSHOP_IND\n" +
                        ",MEDLINE_IND as MEDLINE_IND\n" +
                        ",THOMSON_REUTERS_IND as THOMSON_REUTERS_IND\n" +
                        ",SCOPUS_IND as SCOPUS_IND\n" +
                        ",EMBASE_IND as EMBASE_IND\n" +
                        ",DOAJ_IND as DOAJ_IND\n" +
                        ",ROAD_IND as ROAD_IND\n" +
                        ",PUBMEDCENTRAL_IND as PUBMEDCENTRAL_IND\n" +
                        ",MAIN_LANGUAGE_CODE as MAIN_LANGUAGE_CODE\n" +
                        ",ENGLISH_LANGUAGE_PERCENTAGE_TYPE as ENGLISH_LANGUAGE_PERCENTAGE_TYPE\n" +
                        ",OPEN_ARCHIVE_PERIOD as OPEN_ARCHIVE_PERIOD\n" +
                        ",DELAYED_OPEN_ARCHIVE_IND as DELAYED_OPEN_ARCHIVE_IND\n" +
                        ",INCLUDE_IN_COLLECTIONS_IND as INCLUDE_IN_COLLECTIONS_IND\n" +
                        ",LAUNCH_DATE as LAUNCH_DATE\n" +
                        ",PUBLICATION_SCHEDULE_JAN as PUBLICATION_SCHEDULE_JAN\n" +
                        ",PUBLICATION_SCHEDULE_FEB as PUBLICATION_SCHEDULE_FEB\n" +
                        ",PUBLICATION_SCHEDULE_MAR as PUBLICATION_SCHEDULE_MAR\n" +
                        ",PUBLICATION_SCHEDULE_APR as PUBLICATION_SCHEDULE_APR\n" +
                        ",PUBLICATION_SCHEDULE_MAY as PUBLICATION_SCHEDULE_MAY\n" +
                        ",PUBLICATION_SCHEDULE_JUN as PUBLICATION_SCHEDULE_JUN\n" +
                        ",PUBLICATION_SCHEDULE_JUL as PUBLICATION_SCHEDULE_JUL\n" +
                        ",PUBLICATION_SCHEDULE_AUG as PUBLICATION_SCHEDULE_AUG\n" +
                        ",PUBLICATION_SCHEDULE_SEP as PUBLICATION_SCHEDULE_SEP\n" +
                        ",PUBLICATION_SCHEDULE_OCT as PUBLICATION_SCHEDULE_OCT\n" +
                        ",PUBLICATION_SCHEDULE_NOV as PUBLICATION_SCHEDULE_NOV\n" +
                        ",PUBLICATION_SCHEDULE_DEC as PUBLICATION_SCHEDULE_DEC\n" +
                        ",JOURNAL_ACRONYM_ARGI as JOURNAL_ACRONYM_ARGI\n" +
                        ",MANIFESTATION_FORMATS_CODE as MANIFESTATION_FORMATS_CODE\n" +
                        ",TAKEOVER_YEAR as TAKEOVER_YEAR\n" +
                        ",DOI_PREFIX as DOI_PREFIX\n" +
                        ",DOI_RIGHT_ASSIGNED_IND as DOI_RIGHT_ASSIGNED_IND\n" +
                        ",SOCIETY_OWNED_IND as SOCIETY_OWNED_IND\n" +
                        ",CHECKED_WITH_OA_TEAM_IND as CHECKED_WITH_OA_TEAM_IND\n" +
                        ",IMPRINT_CODE as IMPRINT_CODE\n" +
                        ",DISCONTINUE_DATE as DISCONTINUE_DATE\n" +
                        ",PMX_PRODUCT_WORK_ID as PMX_PRODUCT_WORK_ID\n" +
                        ",REMOVED_FROM_CATALOGUE_IND as REMOVED_FROM_CATALOGUE_IND\n" +
                        ",TRANSFER_DATE as TRANSFER_DATE\n" +
                        ",NOTIFIED_DATE as NOTIFIED_DATE\n" +
                        ",EPH_WORK_ID as EPH_WORK_ID\n" +
                        "from " + databaseEnv[0] + "." + table + "\n" +
                        " where PRODUCT_WORK_ID in ('%s')";
                break;
            case ("aws"):
                GET_DATA_PRODUCT_WORK_JM = "select \n" +
                        "PRODUCT_WORK_ID as PRODUCT_WORK_ID\n" +
                        ",F_PRODUCT_FAMILY as F_PRODUCT_FAMILY\n" +
                        ",PRODUCT_WORK_TITLE as PRODUCT_WORK_TITLE\n" +
                        ",PRODUCT_SUBTITLE as PRODUCT_SUBTITLE\n" +
                        ",PRODUCT_WORK_TITLE_INFO as PRODUCT_WORK_TITLE_INFO\n" +
                        ",ISSN_L as ISSN_L\n" +
                        ",ELSEVIER_JOURNAL_NUMBER as ELSEVIER_JOURNAL_NUMBER\n" +
                        ",INTERNAL_ELSEVIER_DIVISION as INTERNAL_ELSEVIER_DIVISION\n" +
                        ",MANIFESTATION_TYPES_CODE as MANIFESTATION_TYPES_CODE\n" +
                        ",MANIFESTATION_PERSONAL_MODEL_TYPE as MANIFESTATION_PERSONAL_MODEL_TYPE\n" +
                        ",IMPRINT_NAME as IMPRINT_NAME\n" +
                        ",PTS_JOURNAL_INDICATOR as PTS_JOURNAL_INDICATOR\n" +
                        ",YEAR_OF_FIRST_ISSUE as YEAR_OF_FIRST_ISSUE\n" +
                        ",YEAR_OF_LAST_ISSUE as YEAR_OF_LAST_ISSUE\n" +
                        ",FIRST_VOLUME_NAME as FIRST_VOLUME_NAME\n" +
                        ",FIRST_ISSUE_NAME as FIRST_ISSUE_NAME\n" +
                        ",LAST_VOLUME_NAME as LAST_VOLUME_NAME\n" +
                        ",LAST_ISSUE_NAME as LAST_ISSUE_NAME\n" +
                        ",VOLUMES_PER_YEAR_QUANTITY as VOLUMES_PER_YEAR_QUANTITY\n" +
                        ",ISSUES_PER_VOLUME_QUANTITY as ISSUES_PER_VOLUME_QUANTITY\n" +
                        ",FIRST_YEAR_VOLUMES_PER_YEAR_QUANTITY as FIRST_YEAR_VOLUMES_PER_YEAR_QUANTITY\n" +
                        ",FIRST_YEAR_ISSUES_PER_VOLUME_QUANTITY as FIRST_YEAR_ISSUES_PER_VOLUME_QUANTITY\n" +
                        ",PERIODICAL_TIMING_DESC as PERIODICAL_TIMING_DESC\n" +
                        ",OPEN_ACCESSTYPE_CODE as OPEN_ACCESSTYPE_CODE\n" +
                        ",OPEN_ACCESS_SPONSOR_NAME as OPEN_ACCESS_SPONSOR_NAME\n" +
                        ",OPEN_ACCESS_FEE as OPEN_ACCESS_FEE\n" +
                        ",OPEN_ACCESS_CURRENCY_CODE as OPEN_ACCESS_CURRENCY_CODE\n" +
                        ",OPEN_ACCESS_DISCOUNT_IND as OPEN_ACCESS_DISCOUNT_IND\n" +
                        ",OPEN_ACCESS_DISCOUNT_PERIOD as OPEN_ACCESS_DISCOUNT_PERIOD\n" +
                        ",OA_FIRST_YEAR_DISCOUNT_PRICE as OA_FIRST_YEAR_DISCOUNT_PRICE\n" +
                        ",OA_SECOND_YEAR_DISCOUNT_PRICE as OA_SECOND_YEAR_DISCOUNT_PRICE\n" +
                        ",DDP_ELIGIBLE_IND as DDP_ELIGIBLE_IND\n" +
                        ",WEBSHOP_IND as WEBSHOP_IND\n" +
                        ",MEDLINE_IND as MEDLINE_IND\n" +
                        ",THOMSON_REUTERS_IND as THOMSON_REUTERS_IND\n" +
                        ",SCOPUS_IND as SCOPUS_IND\n" +
                        ",EMBASE_IND as EMBASE_IND\n" +
                        ",DOAJ_IND as DOAJ_IND\n" +
                        ",ROAD_IND as ROAD_IND\n" +
                        ",PUBMEDCENTRAL_IND as PUBMEDCENTRAL_IND\n" +
                        ",MAIN_LANGUAGE_CODE as MAIN_LANGUAGE_CODE\n" +
                        ",ENGLISH_LANGUAGE_PERCENTAGE_TYPE as ENGLISH_LANGUAGE_PERCENTAGE_TYPE\n" +
                        ",OPEN_ARCHIVE_PERIOD as OPEN_ARCHIVE_PERIOD\n" +
                        ",DELAYED_OPEN_ARCHIVE_IND as DELAYED_OPEN_ARCHIVE_IND\n" +
                        ",INCLUDE_IN_COLLECTIONS_IND as INCLUDE_IN_COLLECTIONS_IND\n" +
                        ",LAUNCH_DATE as LAUNCH_DATE\n" +
                        ",PUBLICATION_SCHEDULE_JAN as PUBLICATION_SCHEDULE_JAN\n" +
                        ",PUBLICATION_SCHEDULE_FEB as PUBLICATION_SCHEDULE_FEB\n" +
                        ",PUBLICATION_SCHEDULE_MAR as PUBLICATION_SCHEDULE_MAR\n" +
                        ",PUBLICATION_SCHEDULE_APR as PUBLICATION_SCHEDULE_APR\n" +
                        ",PUBLICATION_SCHEDULE_MAY as PUBLICATION_SCHEDULE_MAY\n" +
                        ",PUBLICATION_SCHEDULE_JUN as PUBLICATION_SCHEDULE_JUN\n" +
                        ",PUBLICATION_SCHEDULE_JUL as PUBLICATION_SCHEDULE_JUL\n" +
                        ",PUBLICATION_SCHEDULE_AUG as PUBLICATION_SCHEDULE_AUG\n" +
                        ",PUBLICATION_SCHEDULE_SEP as PUBLICATION_SCHEDULE_SEP\n" +
                        ",PUBLICATION_SCHEDULE_OCT as PUBLICATION_SCHEDULE_OCT\n" +
                        ",PUBLICATION_SCHEDULE_NOV as PUBLICATION_SCHEDULE_NOV\n" +
                        ",PUBLICATION_SCHEDULE_DEC as PUBLICATION_SCHEDULE_DEC\n" +
                        ",JOURNAL_ACRONYM_ARGI as JOURNAL_ACRONYM_ARGI\n" +
                        ",MANIFESTATION_FORMATS_CODE as MANIFESTATION_FORMATS_CODE\n" +
                        ",TAKEOVER_YEAR as TAKEOVER_YEAR\n" +
                        ",DOI_PREFIX as DOI_PREFIX\n" +
                        ",DOI_RIGHT_ASSIGNED_IND as DOI_RIGHT_ASSIGNED_IND\n" +
                        ",SOCIETY_OWNED_IND as SOCIETY_OWNED_IND\n" +
                        ",CHECKED_WITH_OA_TEAM_IND as CHECKED_WITH_OA_TEAM_IND\n" +
                        ",IMPRINT_CODE as IMPRINT_CODE\n" +
                        ",DISCONTINUE_DATE as DISCONTINUE_DATE\n" +
                        ",PMX_PRODUCT_WORK_ID as PMX_PRODUCT_WORK_ID\n" +
                        ",REMOVED_FROM_CATALOGUE_IND as REMOVED_FROM_CATALOGUE_IND\n" +
                        ",TRANSFER_DATE as TRANSFER_DATE\n" +
                        ",NOTIFIED_DATE as NOTIFIED_DATE\n" +
                        ",EPH_WORK_ID as EPH_WORK_ID\n" +
                        "from " + databaseEnv[1] + "." + table + "\n" +
                        " where inbound_ts = (select inbound_ts from " + databaseEnv[1] + "." + table + " order by inbound_ts desc limit 1)\n" +
                        " AND PRODUCT_WORK_ID in ('%s')";
                break;
        }
        return GET_DATA_PRODUCT_WORK_JM;
    }

    public static String getAppAttachSql(String serverEnv, String table) {
        String GET_DATA_APPROVAL_ATTACH_JM = null;
        switch (serverEnv) {
            case ("mysql"):
                GET_DATA_APPROVAL_ATTACH_JM = "select \n" +
                        "APPROVAL_ATTACHMENT_ID as APPROVAL_ATTACHMENT_ID\n" +
                        ",F_APPROVAL as F_APPROVAL\n" +
                        ",ATTACHMENT_NAME as ATTACHMENT_NAME\n" +
                        ",NOTIFIED_DATE as NOTIFIED_DATE\n" +
                        "from " + databaseEnv[0] + "." + table + "\n" +
                        " where APPROVAL_ATTACHMENT_ID in ('%s')";
                break;
            case ("aws"):
                GET_DATA_APPROVAL_ATTACH_JM = "select \n" +
                        "APPROVAL_ATTACHMENT_ID as APPROVAL_ATTACHMENT_ID\n" +
                        ",F_APPROVAL as F_APPROVAL\n" +
                        ",ATTACHMENT_NAME as ATTACHMENT_NAME\n" +
                        ",NOTIFIED_DATE as NOTIFIED_DATE\n" +
                        "from " + databaseEnv[1] + "." + table + "\n" +
                        " where inbound_ts = (select inbound_ts from " + databaseEnv[1] + "." + table + " order by inbound_ts desc limit 1)\n" +
                        " AND APPROVAL_ATTACHMENT_ID in ('%s')";
                break;
        }
        return GET_DATA_APPROVAL_ATTACH_JM;

    }

    public static String getProdInfoSql(String serverEnv, String table) {
        String GET_DATA_PROD_INFO_JM = null;
        switch (serverEnv) {
            case ("mysql"):
                GET_DATA_PROD_INFO_JM = "select \n" +
                        "PRODUCT_WORK_ID as PRODUCT_WORK_ID\n" +
                        ",LE_MANS_IND as LE_MANS_IND\n" +
                        ",SOCIETY_RELATIONSHIP_TYPE as SOCIETY_RELATIONSHIP_TYPE\n" +
                        ",SOCIETY_NAME as SOCIETY_NAME\n" +
                        ",ARTICLE_NUMBER_PER_YEAR as ARTICLE_NUMBER_PER_YEAR\n" +
                        ",SUBMISSION_NUMBER_PER_YEAR as SUBMISSION_NUMBER_PER_YEAR\n" +
                        ",EVISE_REQUESTED_CODE as EVISE_REQUESTED_CODE\n" +
                        ",JOURNAL_ACRONYM_EVISE as JOURNAL_ACRONYM_EVISE\n" +
                        ",JOURNAL_ACRONYM_PTS as JOURNAL_ACRONYM_PTS\n" +
                        ",EVISE_SUPPORT_LEVEL as EVISE_SUPPORT_LEVEL\n" +
                        ",EVISE_WORKFLOW_TYPE as EVISE_WORKFLOW_TYPE\n" +
                        ",EDITOR_LOCATION as EDITOR_LOCATION\n" +
                        ",ABP_USAGE_IND as ABP_USAGE_IND\n" +
                        ",NON_STANDARD_PRODUCTION_ASPECTS as NON_STANDARD_PRODUCTION_ASPECTS\n" +
                        ",EDITORIAL_PRODUCTION_SITE as EDITORIAL_PRODUCTION_SITE\n" +
                        ",PRODUCTION_SPECIFICATION_TYPE as PRODUCTION_SPECIFICATION_TYPE\n" +
                        ",TYPESET_MODEL_TYPE as TYPESET_MODEL_TYPE\n" +
                        ",REFERENCE_STYLE_TYPE as REFERENCE_STYLE_TYPE\n" +
                        ",BUDGETED_PAGE_NUMBER_PER_ISSUE as BUDGETED_PAGE_NUMBER_PER_ISSUE\n" +
                        ",LATEX_SUBMISSION_PERCENTAGE as LATEX_SUBMISSION_PERCENTAGE\n" +
                        ",TYPESETTER_CODE as TYPESETTER_CODE\n" +
                        ",PAGE_REVIEW_CHARGES as PAGE_REVIEW_CHARGES\n" +
                        ",COPY_EDITING_CODE as COPY_EDITING_CODE\n" +
                        ",HISTORY_DATE_FORMAT as HISTORY_DATE_FORMAT\n" +
                        ",PROOF_SENT_TO_AUTHOR_IND as PROOF_SENT_TO_AUTHOR_IND\n" +
                        ",PROOF_SENT_TO_EDITOR_IND as PROOF_SENT_TO_EDITOR_IND\n" +
                        ",EDITOR_EMAIL_ADDRESS as EDITOR_EMAIL_ADDRESS\n" +
                        ",E_SUITE_JOURNAL_IND as E_SUITE_JOURNAL_IND\n" +
                        ",SPONSOR_ACCRESS_REQUIRED_IND as SPONSOR_ACCRESS_REQUIRED_IND\n" +
                        ",ONLINE_PUBLICATION_DATE_IND as ONLINE_PUBLICATION_DATE_IND\n" +
                        ",AUTHOR_FEEDBACK_IND as AUTHOR_FEEDBACK_IND\n" +
                        ",SEND_COPYRIGHT_FORM_IND as SEND_COPYRIGHT_FORM_IND\n" +
                        ",RUNNING_ORDER_DETAILS as RUNNING_ORDER_DETAILS\n" +
                        ",FLEXIBILITY as FLEXIBILITY\n" +
                        ",MAXIMUM_PAGE_DETAILS as MAXIMUM_PAGE_DETAILS\n" +
                        ",SPECIFIC_LOGO_REQUIRED_IND as SPECIFIC_LOGO_REQUIRED_IND\n" +
                        ",ADDITIONAL_DELIVERIES_DETAILS as ADDITIONAL_DELIVERIES_DETAILS\n" +
                        ",MANDATORY_SUBMISSION_ITEM_IND as MANDATORY_SUBMISSION_ITEM_IND\n" +
                        ",DOI_STATEMENT_IND as DOI_STATEMENT_IND\n" +
                        ",LANGUAGE_EDITING_PERFORMED_IND as LANGUAGE_EDITING_PERFORMED_IND\n" +
                        ",LANGUAGE_EDITING_STAGE as LANGUAGE_EDITING_STAGE\n" +
                        ",DEDICATED_JOURNAL_URL_IND as DEDICATED_JOURNAL_URL_IND\n" +
                        ",DEDICATED_JOURNAL_URL as DEDICATED_JOURNAL_URL\n" +
                        ",COI_REQUIRED_IND as COI_REQUIRED_IND\n" +
                        ",EDITORIAL_SYSTEM_NAME as EDITORIAL_SYSTEM_NAME\n" +
                        ",TYPESETTER_NAME as TYPESETTER_NAME\n" +
                        ",EDITORIAL_TURNAROUND_TIME as EDITORIAL_TURNAROUND_TIME\n" +
                        ",PENDING_SUBMISSIONS_QUANTITY as PENDING_SUBMISSIONS_QUANTITY\n" +
                        ",CHECKED_WITH_SOCIETY_TEAM_IND as CHECKED_WITH_SOCIETY_TEAM_IND\n" +
                        ",LAUNCH_DATE as LAUNCH_DATE\n" +
                        ",PROPOSED_EVISE_ROLLOUT_PERIOD_DATE as PROPOSED_EVISE_ROLLOUT_PERIOD_DATE\n" +
                        ",BACKSTOCK_END_YEAR as BACKSTOCK_END_YEAR\n" +
                        ",BACKSTOCK_END_OPTION as BACKSTOCK_END_OPTION\n" +
                        ",JBS_SITE_IND as JBS_SITE_IND\n" +
                        ",EDITORIAL_PRODUCTION_EMAIL_ADDRESS as EDITORIAL_PRODUCTION_EMAIL_ADDRESS\n" +
                        ",EDITORIAL_PRODUCTION_SITE_NAME as EDITORIAL_PRODUCTION_SITE_NAME\n" +
                        ",JOURNAL_HAS_ARTICLE_NOS as JOURNAL_HAS_ARTICLE_NOS\n" +
                        ",JOURNAL_ARTICLE_NUMBER_TYPE as JOURNAL_ARTICLE_NUMBER_TYPE\n" +
                        ",BACKSTOCK_TREATMENT_NOTE as BACKSTOCK_TREATMENT_NOTE\n" +
                        ",NOTIFIED_DATE as NOTIFIED_DATE\n" +
                        "from " + databaseEnv[0] + "." + table + "\n" +
                        " where PRODUCT_WORK_ID in ('%s')";
                break;
            case ("aws"):
                GET_DATA_PROD_INFO_JM = "select \n" +
                        "PRODUCT_WORK_ID as PRODUCT_WORK_ID\n" +
                        ",LE_MANS_IND as LE_MANS_IND\n" +
                        ",SOCIETY_RELATIONSHIP_TYPE as SOCIETY_RELATIONSHIP_TYPE\n" +
                        ",SOCIETY_NAME as SOCIETY_NAME\n" +
                        ",ARTICLE_NUMBER_PER_YEAR as ARTICLE_NUMBER_PER_YEAR\n" +
                        ",SUBMISSION_NUMBER_PER_YEAR as SUBMISSION_NUMBER_PER_YEAR\n" +
                        ",EVISE_REQUESTED_CODE as EVISE_REQUESTED_CODE\n" +
                        ",JOURNAL_ACRONYM_EVISE as JOURNAL_ACRONYM_EVISE\n" +
                        ",JOURNAL_ACRONYM_PTS as JOURNAL_ACRONYM_PTS\n" +
                        ",EVISE_SUPPORT_LEVEL as EVISE_SUPPORT_LEVEL\n" +
                        ",EVISE_WORKFLOW_TYPE as EVISE_WORKFLOW_TYPE\n" +
                        ",EDITOR_LOCATION as EDITOR_LOCATION\n" +
                        ",ABP_USAGE_IND as ABP_USAGE_IND\n" +
                        ",NON_STANDARD_PRODUCTION_ASPECTS as NON_STANDARD_PRODUCTION_ASPECTS\n" +
                        ",EDITORIAL_PRODUCTION_SITE as EDITORIAL_PRODUCTION_SITE\n" +
                        ",PRODUCTION_SPECIFICATION_TYPE as PRODUCTION_SPECIFICATION_TYPE\n" +
                        ",TYPESET_MODEL_TYPE as TYPESET_MODEL_TYPE\n" +
                        ",REFERENCE_STYLE_TYPE as REFERENCE_STYLE_TYPE\n" +
                        ",BUDGETED_PAGE_NUMBER_PER_ISSUE as BUDGETED_PAGE_NUMBER_PER_ISSUE\n" +
                        ",LATEX_SUBMISSION_PERCENTAGE as LATEX_SUBMISSION_PERCENTAGE\n" +
                        ",TYPESETTER_CODE as TYPESETTER_CODE\n" +
                        ",PAGE_REVIEW_CHARGES as PAGE_REVIEW_CHARGES\n" +
                        ",COPY_EDITING_CODE as COPY_EDITING_CODE\n" +
                        ",HISTORY_DATE_FORMAT as HISTORY_DATE_FORMAT\n" +
                        ",PROOF_SENT_TO_AUTHOR_IND as PROOF_SENT_TO_AUTHOR_IND\n" +
                        ",PROOF_SENT_TO_EDITOR_IND as PROOF_SENT_TO_EDITOR_IND\n" +
                        ",EDITOR_EMAIL_ADDRESS as EDITOR_EMAIL_ADDRESS\n" +
                        ",E_SUITE_JOURNAL_IND as E_SUITE_JOURNAL_IND\n" +
                        ",SPONSOR_ACCRESS_REQUIRED_IND as SPONSOR_ACCRESS_REQUIRED_IND\n" +
                        ",ONLINE_PUBLICATION_DATE_IND as ONLINE_PUBLICATION_DATE_IND\n" +
                        ",AUTHOR_FEEDBACK_IND as AUTHOR_FEEDBACK_IND\n" +
                        ",SEND_COPYRIGHT_FORM_IND as SEND_COPYRIGHT_FORM_IND\n" +
                        ",RUNNING_ORDER_DETAILS as RUNNING_ORDER_DETAILS\n" +
                        ",FLEXIBILITY as FLEXIBILITY\n" +
                        ",MAXIMUM_PAGE_DETAILS as MAXIMUM_PAGE_DETAILS\n" +
                        ",SPECIFIC_LOGO_REQUIRED_IND as SPECIFIC_LOGO_REQUIRED_IND\n" +
                        ",ADDITIONAL_DELIVERIES_DETAILS as ADDITIONAL_DELIVERIES_DETAILS\n" +
                        ",MANDATORY_SUBMISSION_ITEM_IND as MANDATORY_SUBMISSION_ITEM_IND\n" +
                        ",DOI_STATEMENT_IND as DOI_STATEMENT_IND\n" +
                        ",LANGUAGE_EDITING_PERFORMED_IND as LANGUAGE_EDITING_PERFORMED_IND\n" +
                        ",LANGUAGE_EDITING_STAGE as LANGUAGE_EDITING_STAGE\n" +
                        ",DEDICATED_JOURNAL_URL_IND as DEDICATED_JOURNAL_URL_IND\n" +
                        ",DEDICATED_JOURNAL_URL as DEDICATED_JOURNAL_URL\n" +
                        ",COI_REQUIRED_IND as COI_REQUIRED_IND\n" +
                        ",EDITORIAL_SYSTEM_NAME as EDITORIAL_SYSTEM_NAME\n" +
                        ",TYPESETTER_NAME as TYPESETTER_NAME\n" +
                        ",EDITORIAL_TURNAROUND_TIME as EDITORIAL_TURNAROUND_TIME\n" +
                        ",PENDING_SUBMISSIONS_QUANTITY as PENDING_SUBMISSIONS_QUANTITY\n" +
                        ",CHECKED_WITH_SOCIETY_TEAM_IND as CHECKED_WITH_SOCIETY_TEAM_IND\n" +
                        ",LAUNCH_DATE as LAUNCH_DATE\n" +
                        ",PROPOSED_EVISE_ROLLOUT_PERIOD_DATE as PROPOSED_EVISE_ROLLOUT_PERIOD_DATE\n" +
                        ",BACKSTOCK_END_YEAR as BACKSTOCK_END_YEAR\n" +
                        ",BACKSTOCK_END_OPTION as BACKSTOCK_END_OPTION\n" +
                        ",JBS_SITE_IND as JBS_SITE_IND\n" +
                        ",EDITORIAL_PRODUCTION_EMAIL_ADDRESS as EDITORIAL_PRODUCTION_EMAIL_ADDRESS\n" +
                        ",EDITORIAL_PRODUCTION_SITE_NAME as EDITORIAL_PRODUCTION_SITE_NAME\n" +
                        ",JOURNAL_HAS_ARTICLE_NOS as JOURNAL_HAS_ARTICLE_NOS\n" +
                        ",JOURNAL_ARTICLE_NUMBER_TYPE as JOURNAL_ARTICLE_NUMBER_TYPE\n" +
                        ",BACKSTOCK_TREATMENT_NOTE as BACKSTOCK_TREATMENT_NOTE\n" +
                        ",NOTIFIED_DATE as NOTIFIED_DATE\n" +
                        "from " + databaseEnv[1] + "." + table + "\n" +
                        " where inbound_ts = (select inbound_ts from " + databaseEnv[1] + "." + table + " order by inbound_ts desc limit 1)\n" +
                        " AND PRODUCT_WORK_ID in ('%s')";
                break;

        }
        return GET_DATA_PROD_INFO_JM;

    }

}




