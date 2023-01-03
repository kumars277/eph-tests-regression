package com.eph.automation.testing.services.db.CKDataLakeSQL;

public class CKEBTDInboundDataChecksSQL {
    //  EBTD Inbound Data IDs SQL
    public static String GET_EBTD_Inbound_VIEW_IDs =
        " select master_isbn as U_KEY from(\n" +
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
                    "  ) order by rand() limit %s";


    //  EBTD Inbound View Data SQL
    public static String GET_EBTD_Inbound_VIEW_Data =
            " select * from(\n" +
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
                    "  ) where master_isbn in ('%s') order by master_isbn desc";
    //  EBTD Inbound View Data SQL
    public static String GET_EBTD_Inbound_TABLE_Data = "select * from " + GetCKDLDB.getCKEBTDDataBase() + ".ck_h300_availability_v " +
            "where master_isbn in ('%s') order by master_isbn desc";

}
