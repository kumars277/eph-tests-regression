package com.eph.automation.testing.services.db.DataLakeSql;

public  class ProductGDGHTablesDataChecksSQL {

    public static String GET_RANDOM_PRODUCT_ID = "select product_id as PRODUCT_ID from semarchy_eph_mdm.gd_product order by random() limit '%s'";
     public static String GET_RANDOM_GH_PRODUCT_ID = "select product_id as PRODUCT_ID from semarchy_eph_mdm.gh_product order by random() limit '%s'";
    public static String GET_RANDOM_ACCOUNTABLE_PRODUCT_ID = "select accountable_product_id from semarchy_eph_mdm.gd_accountable_product order by random() limit '%s'";;


    public static String GET_DATA_PRODUCT_EPH = "select \n" +
            "PRODUCT_ID as PRODUCT_ID\n" +
            ",B_CLASSNAME as B_CLASSNAME\n" +
            ",B_BATCHID as B_BATCHID\n" +
            ",B_CREDATE as B_CREDATE\n" +
            ",B_UPDDATE as B_UPDDATE\n" +
            ",B_CREATOR as B_CREATOR\n" +
            ",B_UPDATOR as B_UPDATOR\n" +
            ",S_PRODUCT_ID as S_PRODUCT_ID\n" +
            ",EXTERNAL_REFERENCE as EXTERNAL_REFERENCE\n" +
            ",NAME as NAME\n" +
            ",S_NAME as S_NAME\n" +
            ",SHORT_NAME as SHORT_NAME\n" +
            ",S_SHORT_NAME as S_SHORT_NAME\n" +
            ",SEPARATELY_SALE_INDICATOR as SEPARATELY_SALE_INDICATOR\n" +
            ",TRIAL_ALLOWED_INDICATOR as TRIAL_ALLOWED_INDICATOR\n" +
            ",LAUNCH_DATE as LAUNCH_DATE\n" +
            ",CONTENT_FROM_DATE as CONTENT_FROM_DATE\n" +
            ",CONTENT_TO_DATE as CONTENT_TO_DATE\n" +
            ",CONTENT_DATE_OFFSET as CONTENT_DATE_OFFSET\n" +
            ",T_SUMMARY_CHANGED as T_SUMMARY_CHANGED\n" +
            ",T_EVENT_DESCRIPTION as T_EVENT_DESCRIPTION\n" +
            ",F_TYPE as F_TYPE\n" +
            ",F_STATUS as F_STATUS\n" +
            ",f_accountable_product as f_accountable_product\n" +
            ",F_TAX_CODE as F_TAX_CODE\n" +
            ",F_REVENUE_MODEL as F_REVENUE_MODEL\n" +
            ",F_REVENUE_ACCOUNT as F_REVENUE_ACCOUNT\n" +
            ",F_WWORK as F_WWORK\n" +
            ",F_MANIFESTATION as F_MANIFESTATION\n" +
            ",F_T_EVENT_TYPE as F_T_EVENT_TYPE\n" +
            ",F_EVENT as F_EVENT\n" +
            ",F_SELF_ONE as F_SELF_ONE\n" +
            ",F_SELF_TWO as F_SELF_TWO\n" +
            ",F_SELF_THREE as F_SELF_THREE\n" +
            ",F_SELF_FOUR as F_SELF_FOUR\n" +
            ",F_SELF_FIVE as F_SELF_FIVE\n" +
            ",F_SELF_SIX as F_SELF_SIX\n" +
            ",F_SELF_SEVEN as F_SELF_SEVEN\n" +
            "from semarchy_eph_mdm.gd_product \n" +
            "where PRODUCT_ID in ('%s')";

    public static String GET_DATA_PRODUCT_DL = "select \n" +
            "PRODUCT_ID as PRODUCT_ID\n" +
            ",B_CLASSNAME as B_CLASSNAME\n" +
            ",B_BATCHID as B_BATCHID\n" +
            ",B_CREDATE as B_CREDATE\n" +
            ",B_UPDDATE as B_UPDDATE\n" +
            ",B_CREATOR as B_CREATOR\n" +
            ",B_UPDATOR as B_UPDATOR\n" +
            ",S_PRODUCT_ID as S_PRODUCT_ID\n" +
            ",EXTERNAL_REFERENCE as EXTERNAL_REFERENCE\n" +
            ",NAME as NAME\n" +
            ",S_NAME as S_NAME\n" +
            ",SHORT_NAME as SHORT_NAME\n" +
            ",S_SHORT_NAME as S_SHORT_NAME\n" +
            ",SEPARATELY_SALE_INDICATOR as SEPARATELY_SALE_INDICATOR\n" +
            ",TRIAL_ALLOWED_INDICATOR as TRIAL_ALLOWED_INDICATOR\n" +
            ",LAUNCH_DATE as LAUNCH_DATE\n" +
            ",CONTENT_FROM_DATE as CONTENT_FROM_DATE\n" +
            ",CONTENT_TO_DATE as CONTENT_TO_DATE\n" +
            ",CONTENT_DATE_OFFSET as CONTENT_DATE_OFFSET\n" +
            ",T_SUMMARY_CHANGED as T_SUMMARY_CHANGED\n" +
            ",T_EVENT_DESCRIPTION as T_EVENT_DESCRIPTION\n" +
            ",F_TYPE as F_TYPE\n" +
            ",F_STATUS as F_STATUS\n" +
            ",f_accountable_product as f_accountable_product\n" +
            ",F_TAX_CODE as F_TAX_CODE\n" +
            ",F_REVENUE_MODEL as F_REVENUE_MODEL\n" +
            ",F_REVENUE_ACCOUNT as F_REVENUE_ACCOUNT\n" +
            ",F_WWORK as F_WWORK\n" +
            ",F_MANIFESTATION as F_MANIFESTATION\n" +
            ",F_T_EVENT_TYPE as F_T_EVENT_TYPE\n" +
            ",F_EVENT as F_EVENT\n" +
            ",F_SELF_ONE as F_SELF_ONE\n" +
            ",F_SELF_TWO as F_SELF_TWO\n" +
            ",F_SELF_THREE as F_SELF_THREE\n" +
            ",F_SELF_FOUR as F_SELF_FOUR\n" +
            ",F_SELF_FIVE as F_SELF_FIVE\n" +
            ",F_SELF_SIX as F_SELF_SIX\n" +
            ",F_SELF_SEVEN as F_SELF_SEVEN\n" +
            " from product_database_sit.gd_product \n" +
            "where PRODUCT_ID in ('%s')";


    public static String GET_DATA_GH_PRODUCT_EPH = "select \n" +
            "PRODUCT_ID as PRODUCT_ID\n" +
            ",B_CLASSNAME as B_CLASSNAME\n" +
            ",B_FROMBATCHID as B_FROMBATCHID\n" +
            ",B_TOBATCHID as B_TOBATCHID\n" +
            ",B_CREDATE as B_CREDATE\n" +
            ",B_UPDDATE as B_UPDDATE\n" +
            ",B_CREATOR as B_CREATOR\n" +
            ",B_UPDATOR as B_UPDATOR\n" +
            ",S_PRODUCT_ID as S_PRODUCT_ID\n" +
            ",EXTERNAL_REFERENCE as EXTERNAL_REFERENCE\n" +
            ",NAME as NAME\n" +
            ",S_NAME as S_NAME\n" +
            ",SHORT_NAME as SHORT_NAME\n" +
            ",S_SHORT_NAME as S_SHORT_NAME\n" +
            ",SEPARATELY_SALE_INDICATOR as SEPARATELY_SALE_INDICATOR\n" +
            ",TRIAL_ALLOWED_INDICATOR as TRIAL_ALLOWED_INDICATOR\n" +
            ",LAUNCH_DATE as LAUNCH_DATE\n" +
            ",CONTENT_FROM_DATE as CONTENT_FROM_DATE\n" +
            ",CONTENT_TO_DATE as CONTENT_TO_DATE\n" +
            ",CONTENT_DATE_OFFSET as CONTENT_DATE_OFFSET\n" +
            ",T_SUMMARY_CHANGED as T_SUMMARY_CHANGED\n" +
            ",T_EVENT_DESCRIPTION as T_EVENT_DESCRIPTION\n" +
            ",F_TYPE as F_TYPE\n" +
            ",F_STATUS as F_STATUS\n" +
            ",f_accountable_product as f_accountable_product\n" +
            ",F_TAX_CODE as F_TAX_CODE\n" +
            ",F_REVENUE_MODEL as F_REVENUE_MODEL\n" +
            ",F_REVENUE_ACCOUNT as F_REVENUE_ACCOUNT\n" +
            ",F_WWORK as F_WWORK\n" +
            ",F_MANIFESTATION as F_MANIFESTATION\n" +
            ",F_T_EVENT_TYPE as F_T_EVENT_TYPE\n" +
            ",F_EVENT as F_EVENT\n" +
            ",F_SELF_ONE as F_SELF_ONE\n" +
            ",F_SELF_TWO as F_SELF_TWO\n" +
            ",F_SELF_THREE as F_SELF_THREE\n" +
            ",F_SELF_FOUR as F_SELF_FOUR\n" +
            ",F_SELF_FIVE as F_SELF_FIVE\n" +
            ",F_SELF_SIX as F_SELF_SIX\n" +
            ",F_SELF_SEVEN as F_SELF_SEVEN\n" +
            "from semarchy_eph_mdm.gh_product \n" +
            "where PRODUCT_ID in ('%s')";

    public static String GET_DATA_GH_PRODUCT_DL = "select \n" +
            "PRODUCT_ID as PRODUCT_ID\n" +
            ",B_CLASSNAME as B_CLASSNAME\n" +
            ",B_FROMBATCHID as B_FROMBATCHID\n" +
            ",B_TOBATCHID as B_TOBATCHID\n" +
            ",B_CREDATE as B_CREDATE\n" +
            ",B_UPDDATE as B_UPDDATE\n" +
            ",B_CREATOR as B_CREATOR\n" +
            ",B_UPDATOR as B_UPDATOR\n" +
            ",S_PRODUCT_ID as S_PRODUCT_ID\n" +
            ",EXTERNAL_REFERENCE as EXTERNAL_REFERENCE\n" +
            ",NAME as NAME\n" +
            ",S_NAME as S_NAME\n" +
            ",SHORT_NAME as SHORT_NAME\n" +
            ",S_SHORT_NAME as S_SHORT_NAME\n" +
            ",SEPARATELY_SALE_INDICATOR as SEPARATELY_SALE_INDICATOR\n" +
            ",TRIAL_ALLOWED_INDICATOR as TRIAL_ALLOWED_INDICATOR\n" +
            ",LAUNCH_DATE as LAUNCH_DATE\n" +
            ",CONTENT_FROM_DATE as CONTENT_FROM_DATE\n" +
            ",CONTENT_TO_DATE as CONTENT_TO_DATE\n" +
            ",CONTENT_DATE_OFFSET as CONTENT_DATE_OFFSET\n" +
            ",T_SUMMARY_CHANGED as T_SUMMARY_CHANGED\n" +
            ",T_EVENT_DESCRIPTION as T_EVENT_DESCRIPTION\n" +
            ",F_TYPE as F_TYPE\n" +
            ",F_STATUS as F_STATUS\n" +
            ",f_accountable_product as f_accountable_product\n" +
            ",F_TAX_CODE as F_TAX_CODE\n" +
            ",F_REVENUE_MODEL as F_REVENUE_MODEL\n" +
            ",F_REVENUE_ACCOUNT as F_REVENUE_ACCOUNT\n" +
            ",F_WWORK as F_WWORK\n" +
            ",F_MANIFESTATION as F_MANIFESTATION\n" +
            ",F_T_EVENT_TYPE as F_T_EVENT_TYPE\n" +
            ",F_EVENT as F_EVENT\n" +
            ",F_SELF_ONE as F_SELF_ONE\n" +
            ",F_SELF_TWO as F_SELF_TWO\n" +
            ",F_SELF_THREE as F_SELF_THREE\n" +
            ",F_SELF_FOUR as F_SELF_FOUR\n" +
            ",F_SELF_FIVE as F_SELF_FIVE\n" +
            ",F_SELF_SIX as F_SELF_SIX\n" +
            ",F_SELF_SEVEN as F_SELF_SEVEN\n" +
            "from product_database_sit.gh_product \n" +
            "where PRODUCT_ID in ('%s')";


    public static String GET_DATA_ACCOUNTABLE_PRODUCT_EPH = "select \n" +
            "ACCOUNTABLE_PRODUCT_ID as ACCOUNTABLE_PRODUCT_ID\n" +
            ",B_CLASSNAME as B_CLASSNAME\n" +
            ",B_BATCHID as B_BATCHID\n" +
            ",B_CREDATE as B_CREDATE\n" +
            ",B_UPDDATE as B_UPDDATE\n" +
            ",B_CREATOR as B_CREATOR\n" +
            ",B_UPDATOR as B_UPDATOR\n" +
            ",EXTERNAL_REFERENCE as EXTERNAL_REFERENCE\n" +
            ",GL_PRODUCT_SEGMENT_CODE as GL_PRODUCT_SEGMENT_CODE\n" +
            ",GL_PRODUCT_SEGMENT_NAME as GL_PRODUCT_SEGMENT_NAME\n" +
            ",F_GL_PRODUCT_SEGMENT_PARENT as F_GL_PRODUCT_SEGMENT_PARENT\n" +
            ",T_EVENT_DESCRIPTION as T_EVENT_DESCRIPTION\n" +
            ",F_T_EVENT_TYPE as F_T_EVENT_TYPE\n" +
            ",F_EVENT as F_EVENT\n" +
            "from semarchy_eph_mdm.gd_accountable_product \n" +
            "where ACCOUNTABLE_PRODUCT_ID in ('%s')";

    public static String GET_DATA_ACCOUNTABLE_PRODUCT_DL = "select \n" +
            "ACCOUNTABLE_PRODUCT_ID as ACCOUNTABLE_PRODUCT_ID\n" +
            ",B_CLASSNAME as B_CLASSNAME\n" +
            ",B_BATCHID as B_BATCHID\n" +
            ",B_CREDATE as B_CREDATE\n" +
            ",B_UPDDATE as B_UPDDATE\n" +
            ",B_CREATOR as B_CREATOR\n" +
            ",B_UPDATOR as B_UPDATOR\n" +
            ",EXTERNAL_REFERENCE as EXTERNAL_REFERENCE\n" +
            ",GL_PRODUCT_SEGMENT_CODE as GL_PRODUCT_SEGMENT_CODE\n" +
            ",GL_PRODUCT_SEGMENT_NAME as GL_PRODUCT_SEGMENT_NAME\n" +
            ",F_GL_PRODUCT_SEGMENT_PARENT as F_GL_PRODUCT_SEGMENT_PARENT\n" +
            ",T_EVENT_DESCRIPTION as T_EVENT_DESCRIPTION\n" +
            ",F_T_EVENT_TYPE as F_T_EVENT_TYPE\n" +
            ",F_EVENT as F_EVENT\n" +
            "from product_database_sit.gd_accountable_product \n" +
            "where ACCOUNTABLE_PRODUCT_ID in ('%s')";
}