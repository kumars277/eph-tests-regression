package com.eph.automation.testing.services.db.DataLakeSql;

import com.eph.automation.testing.configuration.Constants;

public  class PersonGDGHTablesDataChecksSQL {

    public static String GET_RANDOM_PERSON_ID = "select person_id as PERSON_ID from semarchy_eph_mdm.gd_person order by random() limit '%s'";
     public static String GET_RANDOM_GH_PERSON_ID = "select person_id as PERSON_ID from semarchy_eph_mdm.gh_person order by random() limit '%s'";
    public static String GET_RANDOM_GD_PRODUCT_PERSON_ID = "select product_person_role_id as PRODUCT_PERSON_ID from "+Constants.EPH_SCHEMA+".gd_product_person_role order by random() limit '%s'";

     public String gdPersonBuildSql(String db){
         String GET_DATA_PERSON = "select \n" +
                 "PERSON_ID as PERSON_ID\n" +
                 ",B_CLASSNAME as B_CLASSNAME\n" +
                 ",B_BATCHID as B_BATCHID\n" +
                 ",B_CREDATE as B_CREDATE\n" +
                 ",B_UPDDATE as B_UPDDATE\n" +
                 ",B_CREATOR as B_CREATOR\n" +
                 ",B_UPDATOR as B_UPDATOR\n" +
                 ",EXTERNAL_REFERENCE as EXTERNAL_REFERENCE\n" +
                 ",GIVEN_NAME as GIVEN_NAME\n" +
                 ",S_GIVEN_NAME as S_GIVEN_NAME\n" +
                 ",FAMILY_NAME as FAMILY_NAME\n" +
                 ",S_FAMILY_NAME as S_FAMILY_NAME\n" +
                 ",PEOPLEHUB_ID as PEOPLEHUB_ID\n" +
                 ",EMAIL as EMAIL\n" +
                 ",S_EMAIL as S_EMAIL\n" +
                 "from "+db+".gd_person \n" +
                 "where PERSON_ID in ('%s')";

         return GET_DATA_PERSON;
     }

    public String gdProductPersonBuildSql(String db){
        String GET_DATA_PRODUCT_PERSON = "select \n" +
                "PRODUCT_PERSON_ROLE_ID as PRODUCT_PERSON_ROLE_ID\n" +
                ",B_CLASSNAME as B_CLASSNAME\n" +
                ",B_BATCHID as B_BATCHID\n" +
                ",B_CREDATE as B_CREDATE\n" +
                ",B_UPDDATE as B_UPDDATE\n" +
                ",B_CREATOR as B_CREATOR\n" +
                ",B_UPDATOR as B_UPDATOR\n" +
                ",EXTERNAL_REFERENCE as EXTERNAL_REFERENCE\n" +
                ",EFFECTIVE_START_DATE as EFFECTIVE_START_DATE\n" +
                ",EFFECTIVE_END_DATE as EFFECTIVE_END_DATE\n" +
                ",F_ROLE as F_ROLE\n" +
                ",F_PRODUCT as F_PRODUCT\n" +
                ",F_PERSON as F_PERSON\n" +
                ",F_EVENT as F_EVENT\n" +
                "from "+db+".gd_product_person_role \n" +
                "where PRODUCT_PERSON_ROLE_ID in ('%s')";

        return GET_DATA_PRODUCT_PERSON;
    }

    public String ghPersonBuildSql(String db){

        String GET_DATA_GH_PERSON = "select \n" +
                "PERSON_ID as PERSON_ID\n" +
                ",B_CLASSNAME as B_CLASSNAME\n" +
                ",B_FROMBATCHID as B_FROMBATCHID\n" +
                ",B_TOBATCHID as B_TOBATCHID\n" +
                ",B_CREDATE as B_CREDATE\n" +
                ",B_UPDDATE as B_UPDDATE\n" +
                ",B_CREATOR as B_CREATOR\n" +
                ",B_UPDATOR as B_UPDATOR\n" +
                ",EXTERNAL_REFERENCE as EXTERNAL_REFERENCE\n" +
                ",GIVEN_NAME as GIVEN_NAME\n" +
                ",S_GIVEN_NAME as S_GIVEN_NAME\n" +
                ",FAMILY_NAME as FAMILY_NAME\n" +
                ",S_FAMILY_NAME as S_FAMILY_NAME\n" +
                ",PEOPLEHUB_ID as PEOPLEHUB_ID\n" +
                ",EMAIL as EMAIL\n" +
                ",S_EMAIL as S_EMAIL\n" +
                "from "+db+".gh_person \n" +
                "where PERSON_ID in ('%s')";
        return GET_DATA_GH_PERSON;

    }

}