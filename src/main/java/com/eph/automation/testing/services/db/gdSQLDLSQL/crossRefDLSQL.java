package com.eph.automation.testing.services.db.gdSQLDLSQL;


import com.eph.automation.testing.services.db.JMDataLakeSQL.GetJMDLDBUser;

import static com.eph.automation.testing.services.db.JMDataLakeSQL.GetJMDLDBUser.getJMDataBase;

public class crossRefDLSQL {
    static  String[] databaseEnv = getJMDataBase();
     public static String GET_MANIF_GD_SQL_COUNT = "select count(*) as Source_Count from semarchy_eph_mdm.gd_manifestation";
    public static String GET_MANIF_GD_DL_COUNT =  "select count(*) as Target_Count from "+GetJMDLDBUser.getCrossRefDb()+".gd_manifestation";

    public static String GET_MANIF_ID_GD_SQL_COUNT = "select count(*) as Source_Count from semarchy_eph_mdm.gd_manifestation_identifier";


    public static String GET_MANIF_ID_GD_DL_COUNT =  "select count(*) as Target_Count from "+GetJMDLDBUser.getCrossRefDb()+".gd_manifestation_identifier";
    public static String GET_PROD_GD_SQL_COUNT = "select count(*) as Source_Count from semarchy_eph_mdm.gd_product";
    public static String GET_PROD_GD_DL_COUNT =  "select count(*) as Target_Count from "+GetJMDLDBUser.getCrossRefDb()+".gd_product";
     public static String GET_PROD_IDENTIF_GD_SQL_COUNT = "select count(*) as Source_Count from semarchy_eph_mdm.gd_product_identifier";
    public static String GET_PROD_IDENTIF_GD_DL_COUNT =  "select count(*) as Target_Count from "+GetJMDLDBUser.getCrossRefDb()+".gd_product_identifier";
    public static String GET_WORK_IDENTF_GD_SQL_COUNT = "select count(*) as Source_Count from semarchy_eph_mdm.gd_work_identifier";
    public static String GET_WORK_IDENTF_GD_DL_COUNT =  "select count(*) as Target_Count from "+GetJMDLDBUser.getCrossRefDb()+".gd_work_identifier";

    public static String GET_WORK_SQL_COUNT = "select count(*) as Source_Count from semarchy_eph_mdm.gd_wwork";
    public static String GET_WORK_DL_COUNT =  "select count(*) as Target_Count from "+GetJMDLDBUser.getCrossRefDb()+".gd_wwork";

    public static String GET_GD_MANIF_IDS ="select manifestation_id from semarchy_eph_mdm.gd_manifestation order by random() limit %s";
    public static String GET_GD_MANIF_IDENTIF_IDS ="select manif_identifier_id from semarchy_eph_mdm.gd_manifestation_identifier order by random() limit %s";

    public static String GET_GD_PRODUCT_IDS ="select product_id from semarchy_eph_mdm.gd_product order by random() limit %s";

    public static String GET_GD_PRODUCT_IDENTIF_IDS ="select product_identifier_id from semarchy_eph_mdm.gd_product_identifier order by random() limit %s";
    public static String GET_GD_WORK_IDENTIFIER_IDS ="select work_identifier_id from semarchy_eph_mdm.gd_work_identifier order by random() limit %s";
    public static String GET_GD_WORK_IDS ="select work_id from semarchy_eph_mdm.gd_wwork order by random() limit %s";


      public static String GET_GD_MANIFESTATION = "select * from semarchy_eph_mdm.gd_manifestation where manifestation_id in ('%s') order by manifestation_id desc";
    public static String GET_GD_MANIFESTATION_IDENTIFIER = "select * from semarchy_eph_mdm.gd_manifestation_identifier where manif_identifier_id in ('%s') order by manif_identifier_id desc";

    public static String GET_GD_PRODUCT = "select * from semarchy_eph_mdm.gd_product where product_id in ('%s') order by product_id desc";

    public static String GET_GD_PROD_IDENTIFIER = "select * from semarchy_eph_mdm.gd_product_identifier where product_identifier_id in ('%s') order by product_identifier_id desc";
     public static String GET_GD_WORK_IDENTIFIER = "select * from semarchy_eph_mdm.gd_work_identifier where work_identifier_id in ('%s') order by work_identifier_id desc";
     public static String GET_GD_WWORK = "select * from semarchy_eph_mdm.gd_wwork where work_id in ('%s') order by work_id desc";

       public static String GET_GD_MANIFESTATION_DL = "select * from "+GetJMDLDBUser.getCrossRefDb()+".gd_manifestation where manifestation_id in ('%s') order by manifestation_id desc";
    public static String GET_GD_MANIFESTATION_IDENTIFIER_DL = "select * from "+GetJMDLDBUser.getCrossRefDb()+".gd_manifestation_identifier where manif_identifier_id in (%s) order by manif_identifier_id desc";

    public static String GET_GD_PRODUCT_DL = "select * from "+GetJMDLDBUser.getCrossRefDb()+".gd_product where product_id in ('%s') order by product_id desc";

    public static String GET_GD_PROD_IDENTIFIER_DL = "select * from "+GetJMDLDBUser.getCrossRefDb()+".gd_product_identifier where product_identifier_id in (%s) order by product_identifier_id desc";
     public static String GET_GD_WORK_IDENTIFIER_DL = "select * from "+GetJMDLDBUser.getCrossRefDb()+".gd_work_identifier where work_identifier_id in (%s) order by work_identifier_id desc";

    public static String GET_GD_WWORK_DL = "select * from "+GetJMDLDBUser.getCrossRefDb()+".gd_wwork where work_id in ('%s') order by work_id desc";

 public static String GD_WORK_MANIF_PROD_IDENTIFIERS_COUNT =
         "select count (*) as Source_Count from(\n" +
                 "SELECT DISTINCT *\n" +
                 "FROM\n" +
                 "  (\n" +
                 "   WITH\n" +
                 "     work_type AS (\n" +
                 "      SELECT\n" +
                 "        work_id\n" +
                 "      , f_type work_type\n" +
                 "      FROM\n" +
                 "        "+GetJMDLDBUser.getCrossRefDb()+".gd_wwork\n" +
                 "      WHERE (f_status <> 'NVW')\n" +
                 "   ) \n" +
                 ",    manifestation_type AS (\n" +
                 "      SELECT\n" +
                 "        m.manifestation_id\n" +
                 "      , m.f_type manifestation_type\n" +
                 "      , w.f_type work_type\n" +
                 "      , w.work_id work_id\n" +
                 "      FROM\n" +
                 "        ("+GetJMDLDBUser.getCrossRefDb()+".gd_manifestation m\n" +
                 "      INNER JOIN "+GetJMDLDBUser.getCrossRefDb()+".gd_wwork w ON (m.f_wwork = w.work_id))\n" +
                 "      WHERE ((m.f_status <> 'NVM') AND (w.f_status <> 'NVW'))\n" +
                 "   ) \n" +
                 ",    product_type AS (\n" +
                 "      SELECT\n" +
                 "        p.product_id\n" +
                 "      , p.f_type product_type\n" +
                 "      , m.manifestation_type\n" +
                 "      , m.manifestation_id\n" +
                 "      , COALESCE(m.work_type, w.work_type) work_type\n" +
                 "      , COALESCE(m.work_id, w.work_id) work_id\n" +
                 "      FROM\n" +
                 "        (("+GetJMDLDBUser.getCrossRefDb()+".gd_product p\n" +
                 "      LEFT JOIN manifestation_type m ON (p.f_manifestation = m.manifestation_id))\n" +
                 "      LEFT JOIN work_type w ON (p.f_wwork = w.work_id))\n" +
                 "      WHERE (p.f_status <> 'NVP')\n" +
                 "   ) \n" +
                 ",    product_type_wm AS (\n" +
                 "      SELECT\n" +
                 "        p.product_id\n" +
                 "      , p.f_type product_type\n" +
                 "      , m.manifestation_type\n" +
                 "      , m.manifestation_id\n" +
                 "      , m.work_type work_type\n" +
                 "      , m.work_id work_id\n" +
                 "      FROM\n" +
                 "        ("+GetJMDLDBUser.getCrossRefDb()+".gd_product p\n" +
                 "      INNER JOIN manifestation_type m ON (p.f_wwork = m.work_id))\n" +
                 "      WHERE (p.f_status <> 'NVP')\n" +
                 "   ) \n" +
                 "   SELECT\n" +
                 "     wi.identifier\n" +
                 "   , wi.f_type identifier_type\n" +
                 "   , 'Work' record_level\n" +
                 "   , wi.f_wwork epr\n" +
                 "   , null product_type\n" +
                 "   , null manifestation_type\n" +
                 "   , wt.work_type\n" +
                 "   FROM\n" +
                 "     ("+GetJMDLDBUser.getCrossRefDb()+".gd_work_identifier wi\n" +
                 "   INNER JOIN work_type wt ON (wi.f_wwork = wt.work_id))\n" +
                 "   WHERE (wi.effective_end_date IS NULL)\n" +
                 "UNION ALL    SELECT\n" +
                 "     wi.identifier\n" +
                 "   , wi.f_type identifier_type\n" +
                 "   , 'Manifestation' record_level\n" +
                 "   , mt.manifestation_id epr\n" +
                 "   , null product_type\n" +
                 "   , mt.manifestation_type\n" +
                 "   , mt.work_type\n" +
                 "   FROM\n" +
                 "     ("+GetJMDLDBUser.getCrossRefDb()+".gd_work_identifier wi\n" +
                 "   INNER JOIN manifestation_type mt ON (wi.f_wwork = mt.work_id))\n" +
                 "   WHERE (wi.effective_end_date IS NULL)\n" +
                 "UNION ALL    SELECT\n" +
                 "     wi.identifier\n" +
                 "   , wi.f_type identifier_type\n" +
                 "   , 'Product' record_level\n" +
                 "   , pt.product_id epr\n" +
                 "   , pt.product_type\n" +
                 "   , pt.manifestation_type\n" +
                 "   , pt.work_type\n" +
                 "   FROM\n" +
                 "     ("+GetJMDLDBUser.getCrossRefDb()+".gd_work_identifier wi\n" +
                 "   INNER JOIN product_type pt ON (wi.f_wwork = pt.work_id))\n" +
                 "   WHERE (wi.effective_end_date IS NULL)\n" +
                 "UNION ALL    SELECT\n" +
                 "     mi.identifier\n" +
                 "   , mi.f_type identifier_type\n" +
                 "   , 'Manifestation' record_level\n" +
                 "   , mi.f_manifestation epr\n" +
                 "   , null product_type\n" +
                 "   , mt.manifestation_type\n" +
                 "   , mt.work_type\n" +
                 "   FROM\n" +
                 "     ("+GetJMDLDBUser.getCrossRefDb()+".gd_manifestation_identifier mi\n" +
                 "   INNER JOIN manifestation_type mt ON (mi.f_manifestation = mt.manifestation_id))\n" +
                 "   WHERE (mi.effective_end_date IS NULL)\n" +
                 "UNION ALL    SELECT\n" +
                 "     mi.identifier\n" +
                 "   , mi.f_type identifier_type\n" +
                 "   , 'Work' record_level\n" +
                 "   , mt.work_id epr\n" +
                 "   , null product_type\n" +
                 "   , null manifestation_type\n" +
                 "   , mt.work_type\n" +
                 "   FROM\n" +
                 "     ("+GetJMDLDBUser.getCrossRefDb()+".gd_manifestation_identifier mi\n" +
                 "   INNER JOIN manifestation_type mt ON (mi.f_manifestation = mt.manifestation_id))\n" +
                 "   WHERE (mi.effective_end_date IS NULL)\n" +
                 "UNION ALL    SELECT\n" +
                 "     mi.identifier\n" +
                 "   , mi.f_type identifier_type\n" +
                 "   , 'Product' record_level\n" +
                 "   , pt.product_id epr\n" +
                 "   , pt.product_type\n" +
                 "   , pt.manifestation_type\n" +
                 "   , pt.work_type\n" +
                 "   FROM\n" +
                 "     ("+GetJMDLDBUser.getCrossRefDb()+".gd_manifestation_identifier mi\n" +
                 "   INNER JOIN product_type pt ON (mi.f_manifestation = pt.manifestation_id))\n" +
                 "   WHERE (mi.effective_end_date IS NULL)\n" +
                 "UNION ALL    SELECT\n" +
                 "     mi.identifier\n" +
                 "   , mi.f_type identifier_type\n" +
                 "   , 'Product' record_level\n" +
                 "   , pt.product_id epr\n" +
                 "   , pt.product_type\n" +
                 "   , null manifestation_type\n" +
                 "   , pt.work_type\n" +
                 "   FROM\n" +
                 "     ("+GetJMDLDBUser.getCrossRefDb()+".gd_manifestation_identifier mi\n" +
                 "   INNER JOIN product_type_wm pt ON (mi.f_manifestation = pt.manifestation_id))\n" +
                 "   WHERE (mi.effective_end_date IS NULL)\n" +
                 "UNION ALL    SELECT\n" +
                 "     pi.identifier\n" +
                 "   , pi.f_type identifier_type\n" +
                 "   , 'Product' record_level\n" +
                 "   , pi.f_product epr\n" +
                 "   , pt.product_type product_type\n" +
                 "   , null manifestation_type\n" +
                 "   , null work_type\n" +
                 "   FROM\n" +
                 "     ("+GetJMDLDBUser.getCrossRefDb()+".gd_product_identifier pi\n" +
                 "   INNER JOIN product_type pt ON (pi.f_product = pt.product_id))\n" +
                 "   WHERE (pi.effective_end_date IS NULL)\n" +
                 "UNION ALL    SELECT\n" +
                 "     pi.identifier\n" +
                 "   , pi.f_type identifier_type\n" +
                 "   , 'Manifestation' record_level\n" +
                 "   , pt.manifestation_id epr\n" +
                 "   , pt.product_type product_type\n" +
                 "   , pt.manifestation_type\n" +
                 "   , null work_type\n" +
                 "   FROM\n" +
                 "     ("+GetJMDLDBUser.getCrossRefDb()+".gd_product_identifier pi\n" +
                 "   INNER JOIN product_type pt ON (pi.f_product = pt.product_id))\n" +
                 "   WHERE ((pi.effective_end_date IS NULL) AND (pt.manifestation_type IS NOT NULL))\n" +
                 "UNION ALL    SELECT\n" +
                 "     pi.identifier\n" +
                 "   , pi.f_type identifier_type\n" +
                 "   , 'Work' record_level\n" +
                 "   , pt.manifestation_id epr\n" +
                 "   , pt.product_type product_type\n" +
                 "   , pt.manifestation_type\n" +
                 "   , pt.work_type work_type\n" +
                 "   FROM\n" +
                 "     ("+GetJMDLDBUser.getCrossRefDb()+".gd_product_identifier pi\n" +
                 "   INNER JOIN product_type pt ON (pi.f_product = pt.product_id))\n" +
                 "   WHERE ((pi.effective_end_date IS NULL) AND (pt.work_type IS NOT NULL))\n" +
                 "UNION ALL    SELECT\n" +
                 "     pi.identifier\n" +
                 "   , pi.f_type identifier_type\n" +
                 "   , 'Manifestation' record_level\n" +
                 "   , pt.manifestation_id epr\n" +
                 "   , pt.product_type product_type\n" +
                 "   , pt.manifestation_type\n" +
                 "   , null work_type\n" +
                 "   FROM\n" +
                 "     ("+GetJMDLDBUser.getCrossRefDb()+".gd_product_identifier pi\n" +
                 "   INNER JOIN product_type_wm pt ON (pi.f_product = pt.product_id))\n" +
                 "   WHERE (pi.effective_end_date IS NULL)\n" +
                 "UNION ALL    SELECT\n" +
                 "     w.external_reference identifier\n" +
                 "   , 'external_reference' identifier_type\n" +
                 "   , 'Work' record_level\n" +
                 "   , w.work_id epr\n" +
                 "   , null product_type\n" +
                 "   , null manifestation_type\n" +
                 "   , wt.work_type\n" +
                 "   FROM\n" +
                 "     ("+GetJMDLDBUser.getCrossRefDb()+".gd_wwork w\n" +
                 "   INNER JOIN work_type wt ON (w.work_id = wt.work_id))\n" +
                 "   WHERE (w.external_reference IS NOT NULL)\n" +
                 "UNION ALL    SELECT\n" +
                 "     m.external_reference identifier\n" +
                 "   , 'external_reference' identifier_type\n" +
                 "   , 'Manifestation' record_level\n" +
                 "   , m.manifestation_id epr\n" +
                 "   , null product_type\n" +
                 "   , mt.manifestation_type\n" +
                 "   , mt.work_type\n" +
                 "   FROM\n" +
                 "     ("+GetJMDLDBUser.getCrossRefDb()+".gd_manifestation m\n" +
                 "   INNER JOIN manifestation_type mt ON (m.manifestation_id = mt.manifestation_id))\n" +
                 "   WHERE (m.external_reference IS NOT NULL)\n" +
                 "UNION ALL    SELECT\n" +
                 "     p.external_reference identifier\n" +
                 "   , 'external_reference' identifier_type\n" +
                 "   , 'Product' record_level\n" +
                 "   , p.product_id epr\n" +
                 "   , pt.product_type\n" +
                 "   , pt.manifestation_type\n" +
                 "   , pt.work_type\n" +
                 "   FROM\n" +
                 "     ("+GetJMDLDBUser.getCrossRefDb()+".gd_product p\n" +
                 "   INNER JOIN product_type pt ON (p.product_id = pt.product_id))\n" +
                 "   WHERE (p.external_reference IS NOT NULL)\n" +
                 ") as x )xt\n";

    public static String EPH_IDENTIFIER_CROSS_REF_COUNT =
            "select count(*) as Target_Count from "+GetJMDLDBUser.getCrossRefDb()+".eph_identifier_cross_reference";
}

