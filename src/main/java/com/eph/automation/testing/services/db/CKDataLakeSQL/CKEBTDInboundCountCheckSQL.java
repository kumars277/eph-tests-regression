package com.eph.automation.testing.services.db.CKDataLakeSQL;

public class CKEBTDInboundCountCheckSQL {
    //  EBTD View Count SQL
    public static String GET_EBTD_Inbound_COUNT =
            " select count(*) as sourceCount from(\n" +
                    " SELECT\n" +
                    "     e_isbn\n" +
                    "   , master_isbn\n" +
                    "   , h300_date\n" +
                    "   FROM\n" +
                    "     "+GetCKDLDB.getCKEBTDDataBase()+".ck_hs_h300_availability\n" +
                    "   WHERE ((h300_date IS NOT NULL) AND (h300_date <> ''))\n" +
                    "UNION    SELECT\n" +
                    "     e_isbn\n" +
                    "   , master_isbn\n" +
                    "   , h300_date\n" +
                    "   FROM\n" +
                    "     "+GetCKDLDB.getCKEBTDDataBase()+".ck_st_h300_availability\n" +
                    "   WHERE ((h300_date IS NOT NULL) AND (h300_date <> ''))\n" +
                    "   ORDER BY e_isbn ASC, master_isbn ASC, h300_date ASC\n" +
                    "  )";
    //  EBTD Table Count SQL
    public static String GET_EBTD_Inbound_View_COUNT = "select count(*) as targetCount from "+GetCKDLDB.getCKEBTDDataBase()+".%s";
}
