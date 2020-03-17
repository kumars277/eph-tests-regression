package com.eph.automation.testing.services.db.DataLakeSql;

/**
 * Created by Thomas Kruck on 12/03//2020
 */

public class DPPTablesEPHtoDLSQL {
//    COUNT EPH DPP SQL
    public static String EPHCommentCount = "Select count(*) as count from researchpackages.comment";
    public static String EPH_EPHUser = "Select count(*) as count from researchpackages.eph_user";
    public static String EPHPackage = "Select count(*) as count from researchpackages.package";
    public static String EPHPackageApproval = "Select count(*) as count from researchpackages.package_approval";
    public static String EPHPackageApprovals = "Select count(*) as count from researchpackages.package_approvals";
    public static String EPHPackageHaveItems = "Select count(*) as count from researchpackages.package_have_items";
    public static String EPHPackageItem = "Select count(*) as count from researchpackages.package_item";
    public static String EPHPackageItemAudit = "Select count(*) as count from researchpackages.package_Item_Audit";

//    COUNT ATHENA DPP SQL
    public static String DLCommentCount = "Select count(*) as count from product_database_sit.comment";
    public static String DL_EPHUser = "Select count(*) as count from product_database_sit.eph_user";
    public static String DLPackage = "Select count(*) as count from product_database_sit.package";
    public static String DLPackageApproval = "Select count(*) as count from product_database_sit.package_approval";
    public static String DLPackageApprovals = "Select count(*) as count from product_database_sit.package_approvals";
    public static String DLPackageHaveItems = "Select count(*) as count from product_database_sit.package_have_items";
    public static String DLPackageItem = "Select count(*) as count from product_database_sit.package_item";
    public static String DLPackageItemAudit = "Select count(*) as count from product_database_sit.package_Item_Audit";

//  DATA CHECKS RANDOM IDs SQL
  public static String getRandomDPPIDs(String db) {
    String GET_RANDOM_DPP_ID = "select id as ID from " + db + ".%s order by random() limit %s";
        return GET_RANDOM_DPP_ID;
   }
   public static String getRandomDPPPackageApprovalsIDs(String db) {
        String GET_RANDOM_DPP_ID = "select package_approval_id as Package_Approval_ID from " + db + ".%s order by random() limit %s";
        return GET_RANDOM_DPP_ID;
    }

    public static String getRandomDPPPackageItemIDs(String db) {
        String GET_RANDOM_DPP_ID = "select package_item_id as Package_Item_ID from " + db + ".%s order by random() limit %s";
        return GET_RANDOM_DPP_ID;
    }

//    DATA CHECKS DPP TABLE SQL
public static String DPPComment (String db) {
    String GET_DPP_COMMENT = "select \n" +
            "ID as ID\n" +
            ",CREATED_TIME as CREATED_TIME\n" +
            ",LAST_UPDATE_TIME as LAST_UPDATE_TIME\n" +
            ",LAST_UPDATE_USER as LAST_UPDATE_USER\n" +
            ",REASON as REASON\n" +
            ",REMARKS as REMARKS\n" +
            ",STATUS as STATUS\n" +
            "from "+ db + ".%s \n" +
            "where ID in ('%s')";
    return GET_DPP_COMMENT;
}

    public static String DPPEPHUser (String db) {
        String GET_DPP_EPH_USER = "select \n" +
                "ID as ID\n" +
                ",CREATED_TIME as CREATED_TIME\n" +
                ",LAST_UPDATE_TIME as LAST_UPDATE_TIME\n" +
                ",LAST_UPDATE_USER as LAST_UPDATE_USER\n" +
                ",EMAIL as EMAIL\n" +
                ",NAME as NAME\n" +
                ",PEOPLE_HUB_ID as PEOPLE_HUB_ID\n" +
                ",PMG as PMG\n" +
                ",ROLE as ROLE\n" +
                ",UPN_KEY as UPN_KEY\n" +
                "from "+ db + ".%s \n" +
                "where ID in ('%s')";
        return GET_DPP_EPH_USER;
    }

    public static String DPPPackage (String db) {
        String GET_DPP_Package = "select \n" +
                "ID as ID\n" +
                ",CREATED_TIME as CREATED_TIME\n" +
                ",LAST_UPDATE_TIME as LAST_UPDATE_TIME\n" +
                ",LAST_UPDATE_USER as LAST_UPDATE_USER\n" +
                ",PACKAGE_EPR_ID as PACKAGE_EPR_ID\n" +
                ",STATUS as STATUS\n" +
                ",TITLE as TITLE\n" +
                ",VERSION as VERSION\n" +
                ",YEAR as YEAR\n" +
                ",WORKFLOW_TYPE as WORKFLOW_TYPE\n" +
                "from "+ db + ".%s \n" +
                "where ID in ('%s')";
        return GET_DPP_Package;
    }

    public static String DPPPackageApproval(String db) {
        String GET_Package_Approval = "select \n" +
                "ID as ID\n" +
                ",CREATED_TIME as CREATED_TIME\n" +
                ",LAST_UPDATE_TIME as LAST_UPDATE_TIME\n" +
                ",LAST_UPDATE_USER as LAST_UPDATE_USER\n" +
                ",OVERRIDDEN as OVERRIDDEN\n" +
                ",STATUS as STATUS\n" +
                ",EPH_USER_ID as EPH_USER_ID\n" +
                "from "+ db + ".%s \n" +
                "where ID in ('%s')";
        return GET_Package_Approval;
    }

    public static String DPPPackageApprovals(String db) {
        String GET_Package_Approvals = "select \n" +
                "PACKAGE_ID as PACKAGE_ID\n" +
                ",PACKAGE_APPROVAL_ID as PACKAGE_APPROVAL_ID\n" +
                "from "+ db + ".%s \n" +
                "where PACKAGE_APPROVAL_ID in ('%s')";
        return GET_Package_Approvals;
    }

    public static String DPPPackageHaveItems(String db) {
        String GET_Package_Have_Items = "select \n" +
                "PACKAGE_ID as PACKAGE_ID\n" +
                ",PACKAGE_ITEM_ID as PACKAGE_ITEM_ID\n" +
                "from "+ db + ".%s \n" +
                "where PACKAGE_ITEM_ID in ('%s')";
        return GET_Package_Have_Items;
    }

    public static String DPPPackageItem(String db) {
        String GET_Package_Item = "select \n" +
                "ID as ID\n" +
                ",CREATED_TIME as CREATED_TIME\n" +
                ",EPR_ID as EPR_ID\n" +
                ",IS_NEW_ADDITION as IS_NEW_ADDITION\n" +
                ",ISSN as ISSN\n" +
                ",JOURNAL_NUMBER as JOURNAL_NUMBER\n" +
                ",LAST_UPDATE_TIME as LAST_UPDATE_TIME\n" +
                ",LAST_UPDATE_USER as LAST_UPDATE_USER\n" +
                ",OWNERSHIP_TYPE as OWNERSHIP_TYPE\n" +
                ",PMG_CODE as PMG_CODE\n" +
                ",PUBLISHER as PUBLISHER\n" +
                ",PUBLISHING_DIRECTOR as PUBLISHING_DIRECTOR\n" +
                ",CREATED_TIME as CREATED_TIME\n" +
                ",CREATED_TIME as CREATED_TIME\n" +
                ",CREATED_TIME as CREATED_TIME\n" +
                "from "+ db + ".%s \n" +
                "where ID in ('%s')";
        return GET_Package_Item;
    }

    public static String DPPPackageItemAudit(String db) {
        String GET_Package_Item_Audit = "select \n" +
                "ID as ID\n" +
                ",AFTER_VALUE as AFTER_VALUE\n" +
                ",BEFORE_VALUE as BEFORE_VALUE\n" +
                ",CREATED_TIME as CREATED_TIME\n" +
                ",EPR_ID as EPR_ID\n" +
                ",FIELD_NAME as FIELD_NAME\n" +
                ",LAST_UPDATE_TIME as LAST_UPDATE_TIME\n" +
                ",LAST_UPDATE_USER as LAST_UPDATE_USER\n" +
                ",PACKAGE_STATUS as PACKAGE_STATUS\n" +
                ",PACKAGE_ID as PACKAGE_ID\n" +
                "from "+ db + ".%s \n" +
                "where ID in ('%s')";
        return GET_Package_Item_Audit;
    }



}
