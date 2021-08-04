package features.zOnHold.znotused;

import com.eph.automation.testing.services.db.sql.GetEPHDBUser;

public class WorksIdentifierSQL {
    public static String getEphWorkID="SELECT \n" +
            "WORK_ID as WORK_ID FROM semarchy_eph_mdm.sa_wwork\n"  +
            " where f_event =  (select max (f_event) from\n" +
            "semarchy_eph_mdm.sa_wwork join \n"+
            "semarchy_eph_mdm.sa_event on f_event = event_id\n"+
            "where  semarchy_eph_mdm.sa_event.f_event_type = 'PMX'\n"+
            "and semarchy_eph_mdm.sa_event.workflow_id = 'talend'\n"+
            "AND semarchy_eph_mdm.sa_event.f_event_type = 'PMX'\n"+
            "and semarchy_eph_mdm.sa_event.f_workflow_source = 'PMX' )"+
            " AND external_reference='%s'";

    public static String getIdentifierDataFromSA="SELECT \n" +
            " wi.B_LOADID as B_LOADID\n" +
            " ,wi.F_EVENT as F_EVENT\n" +
            " ,wi.B_CLASSNAME as B_CLASSNAME\n" +
            " ,WORK_IDENTIFIER_ID AS WORK_IDENTIFIER_ID -- WORK IDENTIFIER\n" +
            " ,IDENTIFIER AS IDENTIFIER --  IDENTIFIER\n" +
            " ,F_TYPE AS F_TYPE -- WORK IDENTIFIER\n" +
            " ,F_WWORK AS F_WWORK -- WORK IDENTIFIER\n" +
            "  FROM semarchy_eph_mdm.sa_work_identifier wi\n" +
            " where f_event =  (select max (f_event) from\n" +
            "semarchy_eph_mdm.sa_work_identifier join \n"+
            "semarchy_eph_mdm.sa_event on f_event = event_id\n"+
            "where  semarchy_eph_mdm.sa_event.f_event_type = 'PMX'\n"+
            "and semarchy_eph_mdm.sa_event.workflow_id = 'talend'\n"+
            "and semarchy_eph_mdm.sa_event.f_workflow_source = 'PMX' )\n"+
            " AND  f_wwork='%s'\n" +
            "AND effective_end_date is null";

    /*
     // old logic
    public static String getRandomProductNum="SELECT \n" +
            "    \"PRODUCT_WORK_ID\" as random_value\n" +
            "FROM\n" +
            "  "+GetEPHDBUser.getDBUser()+".stg_10_pmx_wwork stg \n" +
            "where \"WORK_TYPE\" = 'PARAM1' \n" +
            "ORDER BY RANDOM()\n" +
            "LIMIT 1";
    */

     // EPH-366 changes  - Introduced DQ layer checks before identifying data to compare
    public static String getRandomProductNum=
             //OLD
//             "SELECT   \n" +
//            "stg.\"PRODUCT_WORK_ID\" as random_value\n" +
//            "from \n" +
//            ""+GetEPHDBUser.getDBUser()+".stg_10_pmx_wwork stg,\n" +
//            ""+GetEPHDBUser.getDBUser()+".stg_10_pmx_wwork_dq wdq\n" +
//            "where \n" +
//            "stg.\"WORK_TYPE\" = '%s' and \n" +
//            "stg.\"PRODUCT_WORK_ID\" = wdq.pmx_source_reference and\n" +
//            "wdq.dq_err != 'Y'\n" +
//            "ORDER BY RANDOM()  LIMIT %s";

   "with\n" +
           "base_data as (\n" +
           "select\n" +
           "   \"PRODUCT_WORK_ID\"  product_work_id\n" +
           "  ,\"ISSN_L\"           issn_l\n" +
           "  ,\"JOURNAL_NUMBER\"   journal_number\n" +
           "  ,\"JOURNAL_ACRONYM\"  journal_acronym\n" +
           "  ,\"DAC_KEY\"          dac_key\n" +
           "  ,\"PROJECT_NUM\"      project_num\n" +
           "  ,map_sourceref_2_ephid('WORK'::varchar,\"PRODUCT_WORK_ID\"::varchar) work_id\n" +
           "  --work_identifier_id\n" +
           "FROM stg_10_pmx_wwork w\n" +
           "join stg_10_pmx_wwork_dq dq on w.\"PRODUCT_WORK_ID\" = dq.pmx_source_reference\n" +
           "where dq.dq_err != 'Y' \n" +
           "and w.\"WORK_TYPE\" = '%s'\n" +
           ")\n" +
           ",crosstab_data as (\n" +
           "    select product_work_id, work_id,'ISSN-L'                  f_type,issn_l          identifier  FROM base_data where issn_l is not null\n" +
           "   union select product_work_id, work_id,'ELSEVIER JOURNAL NUMBER' f_type,journal_number  identifier  FROM base_data where journal_number is not null\n" +
           "   union select product_work_id, work_id,'JOURNAL ACRONYM'\t  f_type,journal_acronym identifier  FROM base_data where journal_acronym is not null\n" +
           "   union select product_work_id, work_id,'DAC-K'  \t          f_type,dac_key         identifier  FROM base_data where dac_key is not null\n" +
           "   union select product_work_id, work_id,'PPM-PART'\t          f_type,project_num     identifier  FROM base_data where project_num is not null\n" +
           ")\n" +
           ",result_data as (\n" +
           "select *\n" +
           "from crosstab_data c\n" +
           "left join semarchy_eph_mdm.gd_work_identifier w on c.work_id = w.f_wwork and c.f_type = w.f_type\n" +
           "left join (select distinct external_reference, work_identifier_id from semarchy_eph_mdm.sa_work_identifier) g on concat(c.work_id,c.f_type,c.identifier) = g.external_reference\n" +
           "where 1=1\n" +
           "and c.identifier != coalesce(w.identifier,'')\n" +
           ")\n" +
           "select product_work_id as random_value\n" +
           "from result_data\n" +
           "order by random()\n" +
           "limit %s";

    public static String getRandomProductNumDelta="SELECT   \n" +
            "stg.\"PRODUCT_WORK_ID\" as random_value\n" +
            "from \n" +
            " "+ GetEPHDBUser.getDBUser()+".stg_10_pmx_wwork stg\n" +
            " join "+GetEPHDBUser.getDBUser()+".stg_10_pmx_wwork_dq  mdq on stg.\"PRODUCT_WORK_ID\" = mdq.PMX_SOURCE_REFERENCE  \n" +
            " left join "+GetEPHDBUser.getDBUser()+".map_sourceref_2_ephid map1  on mdq.pmx_source_reference::text = map1.source_ref \n" +
            " left join semarchy_eph_mdm.gd_work_identifier gwd on gwd.f_wwork = map1.eph_id\n" +
            "where \n" +
            "stg.\"WORK_TYPE\" = 'PARAM1' and \n" +
            "stg.\"PRODUCT_WORK_ID\" = mdq.pmx_source_reference and\n" +
            "mdq.dq_err != 'Y'\n" +
            "   and TO_TIMESTAMP(\"UPDATED\",'YYYYMMDDHH24MI') > TO_TIMESTAMP('PARAM2','YYYYMMDDHH24MI')\n" +
//            "   and TO_TIMESTAMP(\"UPDATED\",'YYYYMMDDHH24MI') > TO_TIMESTAMP('201905201200','YYYYMMDDHH24MI')\n" +
            "   and effective_start_date > TO_DATE('PARAM2','YYYYMMDDHH24MI')\n" +
            "   and  \"PRODUCT_WORK_ID\" \n" +
            "   not in (select distinct stg1.\"PRODUCT_WORK_ID\" from "+GetEPHDBUser.getDBUser()+".stg_10_pmx_wwork stg1,"+GetEPHDBUser.getDBUser()+".stg_10_pmx_wwork stg2  where stg2.\"PRODUCT_WORK_ID\" = stg1.\"PRODUCT_WORK_ID\" and stg2.\"PROJECT_NUM\" != stg1.\"PROJECT_NUM\")\n" +
            "   ORDER BY RANDOM()  LIMIT 1 ";

    //No need to check for DQ error as it is checked while determining the random ID
    public static String getIdentifiers = "SELECT "+
            "  \"JOURNAL_NUMBER\" AS JOURNAL_NUMBER -- Journal Number (may go in IDs table, depending on implementation of data model)\n" +
            "  ,\"ISSN_L\" as ISSN_L-- ISSN-L (may go in IDs table, depending on implementation of data model)\n" +
            "  ,\"JOURNAL_ACRONYM\" AS JOURNAL_ACRONYM -- PTS Journal Acronym (may go in IDs table, depending on implementation of data model)\n" +
            "  ,\"DAC_KEY\" as DAC_KEY-- DAC Key (may go in IDs table, depending on implementation of data model)\n" +
            "  ,\"PROJECT_NUM\" AS PROJECT_NUM -- Project Number (may go in IDs table, depending on implementation of data model)\n" +
            "  ,\"PRODUCT_WORK_ID\" AS PRODUCT_WORK_ID-- Project Number (may go in IDs table, depending on implementation of data model)\n" +
            "  FROM "+GetEPHDBUser.getDBUser()+".stg_10_pmx_wwork\n" +
            "  WHERE \"PRODUCT_WORK_ID\"='%s'";

    public static String getIdentifierDataFromGD= "SELECT \n" +
            " F_EVENT as F_EVENT\n" +
            " ,B_CLASSNAME as B_CLASSNAME\n" +
            " ,WORK_IDENTIFIER_ID AS WORK_IDENTIFIER_ID -- WORK IDENTIFIER\n" +
            " ,IDENTIFIER AS IDENTIFIER --  IDENTIFIER\n" +
            " ,F_TYPE AS F_TYPE -- WORK IDENTIFIER\n" +
            " ,F_WWORK AS F_WWORK -- WORK IDENTIFIER\n" +
            "  FROM semarchy_eph_mdm.gd_work_identifier\n" +
            "  WHERE f_wwork='PARAM1'\n" +
            "  AND effective_end_date is null\n" +
            "  and b_batchid =  (select max (b_batchid) from \n" +
            "semarchy_eph_mdm.gd_event\n" +
            "where  semarchy_eph_mdm.gd_event.f_event_type = 'PMX'\n" +
            "and semarchy_eph_mdm.gd_event.workflow_id = 'talend'\n" +
            "and semarchy_eph_mdm.gd_event.f_workflow_source = 'PMX' )";

    public static String getTypeId="SELECT \n" +
            "   F_TYPE AS F_TYPE -- Identifier type\n" +
            "  ,identifier AS IDENTIFER -- identifier value \n" +
            "  FROM semarchy_eph_mdm.sa_work_identifier\n" +
            "  join semarchy_eph_mdm.sa_event on f_event = event_id and f_event = (select max(f_event) from \n" +
            "            semarchy_eph_mdm.sa_wwork join \n" +
            "            semarchy_eph_mdm.sa_event on f_event = event_id \n" +
            "            where  semarchy_eph_mdm.sa_event.f_event_type = 'PMX'\n" +
            "            and semarchy_eph_mdm.sa_event.workflow_id = 'talend'\n" +
            "            and semarchy_eph_mdm.sa_event.f_workflow_source = 'PMX')\n" +
            "  WHERE f_wwork='PARAM1'"+
            "  AND F_TYPE='PARAM2'\n" +
            "AND effective_end_date is null";

    public static String getTypeIdGD="SELECT \n" +
            "  F_TYPE AS F_TYPE -- WORK IDENTIFIER\n" +
            " ,identifier AS IDENTIFER -- identifier value \n" +
            "  FROM  semarchy_eph_mdm.gd_work_identifier\n" +
            "  WHERE f_wwork='PARAM1'"+
            "  AND F_TYPE='PARAM2'\n" +
            "AND effective_end_date is null";


    public static String GET_F_WWORK = "select eph_id as F_WWORK from "+GetEPHDBUser.getDBUser()+".map_sourceref_2_ephid  where ref_type= 'WORK' and source_ref = '%s' ";

    public static final String COUNT_OF_RECORDS_WITH_ISBN_IN_EPH_STG_WORK_TABLE =


            "select count(distinct \"PRODUCT_WORK_ID\") AS count \n" +
            " from " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_wwork stg ,\n" +
            GetEPHDBUser.getDBUser() + ".stg_10_pmx_wwork_dq  mdq\n" +
            "where \n" +
            "stg.\"%s\" is not null  and \n" +
            "   stg.\"PRODUCT_WORK_ID\" = mdq.PMX_SOURCE_REFERENCE and mdq.dq_err != 'Y' \n";

    public static final String COUNT_OF_RECORDS_WITH_ISBN_IN_EPH_STG_WORK_DELTA =
            "select count(distinct \"PRODUCT_WORK_ID\") AS count \n" +
                    " from " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_wwork stg ,\n" +
                    GetEPHDBUser.getDBUser() + ".stg_10_pmx_wwork_dq  mdq\n" +
                    "where \n" +
                    "stg.\"%s\" is not null  and \n" +
                    "   stg.\"PRODUCT_WORK_ID\" = mdq.PMX_SOURCE_REFERENCE and mdq.dq_err != 'Y' \n" +
                    "and (TO_TIMESTAMP(\"UPDATED\",'YYYYMMDDHH24MI') > TO_TIMESTAMP('%s','YYYYMMDDHH24MI')) \n" ;


//                    " with\n" +
//                    "base_data as (\n" +
//                    "select\n" + "select count(distinct \"PRODUCT_WORK_ID\") AS count \n" +
//                    " from " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_wwork stg ,\n" +
//                    GetEPHDBUser.getDBUser() + ".stg_10_pmx_wwork_dq  mdq\n" +
//                    "where \n" +
//                    "stg.\"%s\" is not null  and \n" +
//                    "   stg.\"PRODUCT_WORK_ID\" = mdq.PMX_SOURCE_REFERENCE and mdq.dq_err != 'Y' \n"
//                    "   \"PRODUCT_WORK_ID\"  product_work_id\n" +
//                    "  ,\"ISSN_L\"           issn_l\n" +
//                    "  ,\"PRODUCT_WORK_ID\"  product_work_id\n" +
//                    "  ,\"JOURNAL_NUMBER\"   journal_number\n" +
//                    "  ,\"JOURNAL_ACRONYM\"  journal_acronym\n" +
//                    "  ,\"DAC_KEY\"          dac_key\n" +
//                    "  ,\"PROJECT_NUM\"      project_num\n" +
//                    "  ,map_sourceref_2_ephid('WORK'::varchar,\"PRODUCT_WORK_ID\"::varchar) work_id\n" +
//                    "  --work_identifier_id\n" +
//                    "FROM stg_10_pmx_wwork w\n" +
//                    "join stg_10_pmx_wwork_dq dq on w.\"PRODUCT_WORK_ID\" = dq.pmx_source_reference\n" +
//                    "where dq.dq_err != 'Y'\n" +
//                    "and (TO_TIMESTAMP(w.\"UPDATED\",'YYYYMMDDHH24MI') > TO_TIMESTAMP('%s','YYYYMMDDHH24MI') \n" +
////                    "    and TO_TIMESTAMP(\"UPDATED\",'YYYYMMDDHH24MI') > TO_TIMESTAMP('201905201200','YYYYMMDDHH24MI')\n" +
//                    ")\n" +
//                    ",crosstab_data as (\n" +
//                    "       select work_id,'%s'                  f_type,%s          identifier  FROM base_data where %s  is not null\n" +
//                    ")\n" +
//                    ",result_data as (\n" +
//                    "select\n" +
//                    "*\n" +
//                    "from crosstab_data c\n" +
//                    "left join semarchy_eph_mdm.gd_work_identifier w on c.work_id = w.f_wwork and c.f_type = w.f_type\n" +
//                    "left join (select distinct external_reference, work_identifier_id from semarchy_eph_mdm.sa_work_identifier) g on concat(c.work_id,c.f_type,c.identifier) = g.external_reference\n" +
//                    "where 1=1\n" +
//                    "and c.identifier != coalesce(w.identifier,'')\n" +
//                    ")\n" +
//                    "select count(*)\n" +
//                    "from result_data";
            //old
//            "select count(*) as count\n" +
//            "from  "+GetEPHDBUser.getDBUser()+".stg_10_pmx_wwork stg \n" +
//            " join "+GetEPHDBUser.getDBUser()+".stg_10_pmx_wwork_dq  mdq on stg.\"PRODUCT_WORK_ID\" = mdq.PMX_SOURCE_REFERENCE  \n" +
//            " left join "+GetEPHDBUser.getDBUser()+".map_sourceref_2_ephid map1  on mdq.pmx_source_reference::text = map1.source_ref \n" +
//            " left join semarchy_eph_mdm.gd_work_identifier gwd on gwd.f_wwork = map1.eph_id\n" +
//            "where \n" +
//            "stg.\"PARAM1\" is not null  and  mdq.dq_err != 'Y'\n" +
////            "   and TO_TIMESTAMP(\"UPDATED\",'YYYYMMDDHH24MI') > TO_TIMESTAMP('PARAM2','YYYYMMDDHH24MI')\n" +
//            "   and TO_TIMESTAMP(\"UPDATED\",'YYYYMMDDHH24MI') > TO_TIMESTAMP('201905201200','YYYYMMDDHH24MI')\n" +
//            "   and effective_start_date > TO_DATE('PARAM2','YYYYMMDDHH24MI')\n" +
//            "   and gwd.f_type = 'PARAM3'\n" +
//            "   and gwd.identifier = stg.\"PARAM1\"\n" +
//            "   and  \"PRODUCT_WORK_ID\" \n" +
//            "   not in (select distinct stg1.\"PRODUCT_WORK_ID\" from "+GetEPHDBUser.getDBUser()+".stg_10_pmx_wwork stg1,"+GetEPHDBUser.getDBUser()+".stg_10_pmx_wwork stg2  where stg2.\"PRODUCT_WORK_ID\" = stg1.\"PRODUCT_WORK_ID\" and stg2.\"PROJECT_NUM\" != stg1.\"PROJECT_NUM\");";



    public static String COUNT_SA_WORK_IDENTIFIER = "select count(distinct f_wwork) AS count from semarchy_eph_mdm.sa_work_identifier where f_type = 'PARAM1'" +
            " and b_error_status is null\n" +
            " and effective_end_date is null\n" +
            " and f_event =  (select max (event_id) from\n" +
            "semarchy_eph_mdm.sa_event\n"+
            "where  semarchy_eph_mdm.sa_event.f_event_type = 'PMX'\n"+
            "and semarchy_eph_mdm.sa_event.workflow_id = 'talend'\n"+
            "and semarchy_eph_mdm.sa_event.f_workflow_source = 'PMX' )";

    public static String COUNT_GD_WORK_IDENTIFIER = "select count(distinct f_wwork) AS count from semarchy_eph_mdm.gd_work_identifier where f_type = 'PARAM1'"+
            " and effective_end_date is null\n" +
            " and b_batchid =  (select max (b_batchid) from\n" +
            "semarchy_eph_mdm.gd_event\n"+
            "where  semarchy_eph_mdm.gd_event.f_event_type = 'PMX'\n"+
            "and semarchy_eph_mdm.gd_event.workflow_id = 'talend'\n"+
            "and semarchy_eph_mdm.gd_event.f_workflow_source = 'PMX' )";

    public static String getEndDatedIdentifierDataFromGD="SELECT \n" +
            " F_EVENT as F_EVENT\n" +
            " ,B_CLASSNAME as B_CLASSNAME\n" +
            " ,WORK_IDENTIFIER_ID AS WORK_IDENTIFIER_ID -- WORK IDENTIFIER\n" +
            " ,IDENTIFIER AS IDENTIFIER --  IDENTIFIER\n" +
            " ,F_TYPE AS F_TYPE -- WORK IDENTIFIER\n" +
            " ,F_WWORK AS F_WWORK -- WORK IDENTIFIER\n" +
            "  FROM semarchy_eph_mdm.gd_work_identifier\n" +
            "  where effective_end_date is not null\n"+
            " AND b_batchid =  (select max (b_batchid) from\n" +
            "semarchy_eph_mdm.gd_event\n"+
            "where  semarchy_eph_mdm.gd_event.f_event_type = 'PMX'\n"+
            "and semarchy_eph_mdm.gd_event.workflow_id = 'talend'\n"+
            "and semarchy_eph_mdm.gd_event.f_workflow_source = 'PMX' ) ORDER BY RANDOM()\n" +
            " LIMIT PARAM1;";

    public static String getEndDatedIdentifierData="SELECT \n" +
            " F_EVENT as F_EVENT\n" +
            " ,B_CLASSNAME as B_CLASSNAME\n" +
            " ,WORK_IDENTIFIER_ID AS WORK_IDENTIFIER_ID -- WORK IDENTIFIER\n" +
            " ,IDENTIFIER AS IDENTIFIER --  IDENTIFIER\n" +
            " ,F_TYPE AS F_TYPE -- WORK IDENTIFIER\n" +
            " ,F_WWORK AS F_WWORK -- WORK IDENTIFIER\n" +
            "  FROM semarchy_eph_mdm.gd_work_identifier\n" +
            "  where effective_end_date is not null\n"+
            " AND b_batchid =  (select max (b_batchid) from\n" +
            "semarchy_eph_mdm.gd_event\n"+
            "where  semarchy_eph_mdm.gd_event.f_event_type = 'PMX'\n"+
            "and semarchy_eph_mdm.gd_event.workflow_id = 'talend'\n"+
            "and semarchy_eph_mdm.gd_event.f_workflow_source = 'PMX' )\n" +
            " and f_wwork = 'PARAM1';";


    public static String getPmxSourceRef="SELECT \n" +
            "external_reference as WORK_ID FROM semarchy_eph_mdm.gd_wwork\n"  +
            "where f_event =  (select max (f_event) from\n" +
            "semarchy_eph_mdm.gd_wwork join \n"+
            "semarchy_eph_mdm.gd_event on f_event = event_id\n"+
            "where  semarchy_eph_mdm.gd_event.f_event_type = 'PMX'\n"+
            "and semarchy_eph_mdm.gd_event.workflow_id = 'talend'\n"+
            "and semarchy_eph_mdm.gd_event.f_workflow_source = 'PMX' )"+
            " AND work_id = 'PARAM1'";

    public static String getIdentifiersDelta = "SELECT "+
            "  \"JOURNAL_NUMBER\" AS JOURNAL_NUMBER -- Journal Number (may go in IDs table, depending on implementation of data model)\n" +
            "  ,\"ISSN_L\" as ISSN_L-- ISSN-L (may go in IDs table, depending on implementation of data model)\n" +
            "  ,\"JOURNAL_ACRONYM\" AS JOURNAL_ACRONYM -- PTS Journal Acronym (may go in IDs table, depending on implementation of data model)\n" +
            "  ,\"DAC_KEY\" as DAC_KEY-- DAC Key (may go in IDs table, depending on implementation of data model)\n" +
            "  ,\"PROJECT_NUM\" AS PROJECT_NUM -- Project Number (may go in IDs table, depending on implementation of data model)\n" +
            "  ,\"PRODUCT_WORK_ID\" AS PRODUCT_WORK_ID-- Project Number (may go in IDs table, depending on implementation of data model)\n" +
            "  FROM "+GetEPHDBUser.getDBUser()+".stg_10_pmx_wwork\n" +
            "  WHERE \"PRODUCT_WORK_ID\" = 'PARAM1'";

    public static String getIdentifierID ="select sman.external_reference as \"SA\" , concat(map1.eph_id,'PARAM1',man.\"PARAM2\") as \"STG\"   from " + GetEPHDBUser.getDBUser() +".stg_10_pmx_wwork man, " + GetEPHDBUser.getDBUser() +".stg_10_pmx_wwork_dq mdq , semarchy_eph_mdm.sa_work_identifier sman \n" +
            ", " + GetEPHDBUser.getDBUser() +".map_sourceref_2_ephid map1 \n" +
            "where man.\"PRODUCT_WORK_ID\" = mdq.pmx_source_reference\n" +
            "and map1.source_ref = mdq.pmx_source_reference::text\n" +
            "and concat(map1.eph_id,'PARAM1',man.\"PARAM2\") = sman.external_reference\n" +
            "and f_wwork = 'PARAM3'";

}
