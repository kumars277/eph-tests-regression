package com.eph.automation.testing.services.db.sql;
//created by Nishant @ 29 Jan 2021 for EPHD-2747

public class stitchingDataSQL {
    public static String GETRandomIds_stch_manifestation_ext_json=
            "select * from (select smej.epr_id, smej.\"json\" from eph%s_extended_data_stitch.stch_manifestation_ext_json smej  \n" +
                    "inner join semarchy_eph_mdm.gd_product gp on smej.epr_id =gp.f_manifestation \n" +
                    "order by random() limit %s)as result_set order by epr_id asc";

    public static String GETRandomIds_stch_product_core_json=
            "select * from(select  epr_id , \"json\" from eph%s_extended_data_stitch.stch_product_core_json " +
                    "order by random() limit %s) as result_set order by epr_id desc";

    public static String GETRandomIds_stch_product_ext_json_byAvailability=
            "select * from(select  epr_id ,\"json\" from eph%s_extended_data_stitch.stch_product_ext_json " +
                    "where extension_type = 'Availability' order by random() limit %s)as result_set order by epr_id desc";

    public static String GETRandomIds_stch_product_ext_json_byPrices=
            "select * from(select  epr_id ,\"json\" from eph%s_extended_data_stitch.stch_product_ext_json " +
                    "where extension_type = 'Prices' order by random() limit %s) as result_set order by epr_id desc";

    public static String GETRandomIds_stch_work_core_json=
            "select * from (select swcj.epr_id, swcj.\"json\" from eph%s_extended_data_stitch.stch_work_core_json swcj  \n" +
                    "inner join semarchy_eph_mdm.gd_product gp on swcj.epr_id =gp.f_wwork \n" +
                    "order by random() limit %s)as result_set order by epr_id asc";

    public static String GETRandomIds_stch_work_ext_json=
            "select * from (select swej.epr_id, swej.\"json\" from eph%s_extended_data_stitch.stch_work_ext_json swej  \n" +
                    "inner join semarchy_eph_mdm.gd_product gp on swej.epr_id =gp.f_wwork \n" +
                    "order by random() limit %s)as result_set order by epr_id asc";



    public static String getStitchingData_stch_manifestation_ext_json=
            "select  epr_id , \"json\" from eph%s_extended_data_stitch.stch_manifestation_ext_json where epr_id='%s'";

    public static String getStitchingData_stch_product_core_json=
            "select  epr_id , \"json\" from eph%s_extended_data_stitch.stch_product_core_json where epr_id='%s'";

    public static String getStitchingData_stch_product_ext_json_byAvailability=
            "select  epr_id , \"json\" from eph%s_extended_data_stitch.stch_product_ext_json where extension_type = 'Availability' and epr_id='%s'";

    public static String getStitchingData_stch_product_ext_json_byPrices=
            "select  epr_id , \"json\" from eph%s_extended_data_stitch.stch_product_ext_json where extension_type = 'Prices' and epr_id='%s'";

    public static String getStitchingData_stch_work_core_json=
            "select  epr_id , \"json\" from eph%s_extended_data_stitch.stch_work_core_json where epr_id='%s'";

    public static String getStitchingData_stch_work_ext_json=
            "select  epr_id , \"json\" from eph%s_extended_data_stitch.stch_work_ext_json where epr_id='%s'";


}
