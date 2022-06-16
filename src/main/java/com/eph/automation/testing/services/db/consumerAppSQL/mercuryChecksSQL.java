package com.eph.automation.testing.services.db.consumerAppSQL;


import com.eph.automation.testing.services.db.PROMISDataLakeSQL.GetPRMDLDBUser;
import com.eph.automation.testing.services.db.JMDataLakeSQL.GetJMDLDBUser;

public class mercuryChecksSQL {
     private mercuryChecksSQL() {
         throw new IllegalStateException("Utility class");
     }

       public static String GET_SOURCE_EPH_COUNT =
              "Select count(*) as source_count from(\n" +
                      "SELECT *\n" +
                      "FROM\n" +
                      "  (\n" +
                      "   SELECT DISTINCT\n" +
                      "     '3.1' schemaversion\n" +
                      "   , least(COALESCE(p.b_credate, date_parse('9999-01-01 00:00:00.000', '%Y-%m-%d %H:%i:%s.%f')), least(COALESCE(w.b_credate, date_parse('9999-01-01 00:00:00.000', '%Y-%m-%d %H:%i:%s.%f')), least(COALESCE(m.b_credate, date_parse('9999-01-01 00:00:00.000', '%Y-%m-%d %H:%i:%s.%f')), least(COALESCE(w_id1.b_credate, date_parse('9999-01-01 00:00:00.000', '%Y-%m-%d %H:%i:%s.%f')), least(COALESCE(w_id2.b_credate, date_parse('9999-01-01 00:00:00.000', '%Y-%m-%d %H:%i:%s.%f')), least(COALESCE(m_id.b_credate, date_parse('9999-01-01 00:00:00.000', '%Y-%m-%d %H:%i:%s.%f')), COALESCE(fa.b_credate, date_parse('9999-01-01 00:00:00.000', '%Y-%m-%d %H:%i:%s.%f')))))))) created\n" +
                      "   , greatest(COALESCE(p.b_upddate, date_parse('1900-01-01 00:00:00.000', '%Y-%m-%d %H:%i:%s.%f')), greatest(COALESCE(w.b_upddate, date_parse('1900-01-01 00:00:00.000', '%Y-%m-%d %H:%i:%s.%f')), greatest(COALESCE(m.b_upddate, date_parse('1900-01-01 00:00:00.000', '%Y-%m-%d %H:%i:%s.%f')), greatest(COALESCE(w_id1.b_upddate, date_parse('1900-01-01 00:00:00.000', '%Y-%m-%d %H:%i:%s.%f')), greatest(COALESCE(w_id2.b_upddate, date_parse('1900-01-01 00:00:00.000', '%Y-%m-%d %H:%i:%s.%f')), greatest(COALESCE(m_id.b_upddate, date_parse('1900-01-01 00:00:00.000', '%Y-%m-%d %H:%i:%s.%f')), COALESCE(fa.b_upddate, date_parse('1900-01-01 00:00:00.000', '%Y-%m-%d %H:%i:%s.%f')))))))) updated\n" +
                      "   , p.product_id product_id\n" +
                      "   , p.name product_full_name\n" +
                      "   , p.short_name product_name\n" +
                      "   , w.work_title work_title\n" +
                      "   , wt.l_description work_type\n" +
                      "   , wt.roll_up_type work_typerollup\n" +
                      "   , ws.l_description work_status\n" +
                      "   , ws.roll_up_status work_statusrollup\n" +
                      "   , m.manifestation_id manifestation_id\n" +
                      "   , w_id1.identifier journal_number\n" +
                      "   , w_id2.identifier journal_acronym\n" +
                      "   , fa.f_gl_company operating_company\n" +
                      "   , (CASE WHEN (p.f_type = 'PPV') THEN 'PPV' WHEN (m.f_type = 'JPR') THEN 'Print' WHEN (m.f_type = 'JEL') THEN 'Online' ELSE 'Unknown' END) product_type\n" +
                      "   , m_id.identifier issn\n" +
                      "   , ps.roll_up_status publication_status\n" +
                      "   , p.f_tax_code etax_code\n" +
                      "   , mpa.fulfilment_system fulfilment_system\n" +
                      "   , mpa.warehouse_primary warehouse_primary\n" +
                      "   , mpa.warehouse_backissue warehouse_backissue\n" +
                      "   , mpa.number_of_issues number_of_issues\n" +
                      "   , mpa.pod_supplier pod_supplier\n" +
                      "   , mpa.ddp_eligible ddp_eligible\n" +
                      "   , COALESCE(jmupd.pts_journal_indicator, mpa.pts_journal_indicator) pts_journal_indicator\n" +
                      "   , jmupd.year_of_first_issue year_of_first_issue\n" +
                      "   , jmupd.first_issue_name first_issue_name\n" +
                      "   , jmupd.first_volume_name first_volume_name\n" +
                      "   , (CASE WHEN (w.f_subscription_type IS NULL) THEN 'RY' ELSE w.f_subscription_type END) default_start\n" +
                      "   , 'TBC' short_title\n" +
                      "   , fa.f_gl_cost_resp_centre rc_code\n" +
                      "   , w.f_pmc pmc_code\n" +
                      "   FROM\n" +
                      "     ((((((((((("+GetPRMDLDBUser.getProdDataBase()+".gd_product p\n" +
                      "   LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_manifestation m ON (p.f_manifestation = m.manifestation_id))\n" +
                      "   LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_wwork w ON ((CASE WHEN (p.f_manifestation IS NULL) THEN p.f_wwork ELSE m.f_wwork END) = w.work_id))\n" +
                      "   LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_work_type wt ON (w.f_type = wt.code))\n" +
                      "   LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_work_status ws ON (w.f_status = ws.code))\n" +
                      "   LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_product_status ps ON (p.f_status = ps.code))\n" +
                      "   LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_work_identifier w_id1 ON ((w_id1.f_wwork = w.work_id) AND ((w_id1.f_type = 'ELSEVIER JOURNAL NUMBER') AND (w_id1.effective_end_date IS NULL))))\n" +
                      "   LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_work_identifier w_id2 ON ((w_id2.f_wwork = w.work_id) AND ((w_id2.f_type = 'JOURNAL ACRONYM') AND (w_id2.effective_end_date IS NULL))))\n" +
                      "   LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_work_financial_attribs fa ON ((fa.f_wwork = w.work_id) AND (fa.effective_end_date IS NULL)))\n" +
                      "   LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_manifestation_identifier m_id ON (((m_id.f_manifestation = m.manifestation_id) AND (m_id.f_type = 'ISSN')) AND (m_id.effective_end_date IS NULL)))\n" +
                      "   INNER JOIN "+GetPRMDLDBUser.getProdDataBase()+".mercury_print_addl mpa ON (mpa.product_id = p.product_id))\n" +
                      "   LEFT JOIN (\n" +
                      "      SELECT\n" +
                      "        work.elsevier_journal_number\n" +
                      "      , work.pts_journal_indicator\n" +
                      "      , work.year_of_first_issue\n" +
                      "      , work.first_issue_name\n" +
                      "      , work.first_volume_name\n" +
                      "      FROM\n" +
                      "        ("+GetJMDLDBUser.getJMDB()+".jmf_work work\n" +
                      "      INNER JOIN "+GetJMDLDBUser.getJMDB()+".jmf_work_chronicle chronicle ON (chronicle.work_chronicle_id = work.work_chronicle_id))\n" +
                      "      WHERE (((chronicle.chronicle_scenario_code = 'RN') AND (work.work_journey_identifier = 'A1')) AND (chronicle.notified_date = (SELECT max(chronicle2.notified_date)\n" +
                      "FROM\n" +
                      "  ("+GetJMDLDBUser.getJMDB()+".jmf_work work2\n" +
                      "INNER JOIN "+GetJMDLDBUser.getJMDB()+".jmf_work_chronicle chronicle2 ON (chronicle2.work_chronicle_id = work2.work_chronicle_id))\n" +
                      "WHERE ((chronicle2.chronicle_scenario_code = chronicle.chronicle_scenario_code) AND (work2.elsevier_journal_number = work.elsevier_journal_number))\n" +
                      ")))\n" +
                      "   )  jmupd ON (jmupd.elsevier_journal_number = w_id1.identifier))\n" +
                      "   WHERE ((wt.roll_up_type = 'Journal') AND (p.f_type IN ('PPV', 'SUB')))\n" +
                      "UNION (\n" +
                      "      SELECT *\n" +
                      "      FROM\n" +
                      "        (\n" +
                      "         SELECT DISTINCT\n" +
                      "           '3.1' schemaversion\n" +
                      "         , least(COALESCE(p.b_credate, date_parse('9999-01-01 00:00:00.000', '%Y-%m-%d %H:%i:%s.%f')), least(COALESCE(w.b_credate, date_parse('9999-01-01 00:00:00.000', '%Y-%m-%d %H:%i:%s.%f')), least(COALESCE(m.b_credate, date_parse('9999-01-01 00:00:00.000', '%Y-%m-%d %H:%i:%s.%f')), least(COALESCE(w_id1.b_credate, date_parse('9999-01-01 00:00:00.000', '%Y-%m-%d %H:%i:%s.%f')), least(COALESCE(w_id2.b_credate, date_parse('9999-01-01 00:00:00.000', '%Y-%m-%d %H:%i:%s.%f')), least(COALESCE(m_id.b_credate, date_parse('9999-01-01 00:00:00.000', '%Y-%m-%d %H:%i:%s.%f')), COALESCE(fa.b_credate, date_parse('9999-01-01 00:00:00.000', '%Y-%m-%d %H:%i:%s.%f')))))))) created\n" +
                      "         , greatest(COALESCE(p.b_upddate, date_parse('1900-01-01 00:00:00.000', '%Y-%m-%d %H:%i:%s.%f')), greatest(COALESCE(w.b_upddate, date_parse('1900-01-01 00:00:00.000', '%Y-%m-%d %H:%i:%s.%f')), greatest(COALESCE(m.b_upddate, date_parse('1900-01-01 00:00:00.000', '%Y-%m-%d %H:%i:%s.%f')), greatest(COALESCE(w_id1.b_upddate, date_parse('1900-01-01 00:00:00.000', '%Y-%m-%d %H:%i:%s.%f')), greatest(COALESCE(w_id2.b_upddate, date_parse('1900-01-01 00:00:00.000', '%Y-%m-%d %H:%i:%s.%f')), greatest(COALESCE(m_id.b_upddate, date_parse('1900-01-01 00:00:00.000', '%Y-%m-%d %H:%i:%s.%f')), COALESCE(fa.b_upddate, date_parse('1900-01-01 00:00:00.000', '%Y-%m-%d %H:%i:%s.%f')))))))) updated\n" +
                      "         , p.product_id product_id\n" +
                      "         , p.name product_full_name\n" +
                      "         , p.short_name product_name\n" +
                      "         , w.work_title work_title\n" +
                      "         , wt.l_description work_type\n" +
                      "         , wt.roll_up_type work_typerollup\n" +
                      "         , ws.l_description work_status\n" +
                      "         , ws.roll_up_status work_statusrollup\n" +
                      "         , m.manifestation_id manifestation_id\n" +
                      "         , w_id1.identifier journal_number\n" +
                      "         , w_id2.identifier journal_acronym\n" +
                      "         , fa.f_gl_company operating_company\n" +
                      "         , (CASE WHEN (p.f_type = 'PPV') THEN 'PPV' WHEN (m.f_type = 'JPR') THEN 'Print' WHEN (m.f_type = 'JEL') THEN 'Online' ELSE 'Unknown' END) product_type\n" +
                      "         , m_id.identifier issn\n" +
                      "         , ps.roll_up_status publication_status\n" +
                      "         , p.f_tax_code etax_code\n" +
                      "         , (CASE WHEN (jmm.application_code IN ('AMPS', 'QSS', 'Argi')) THEN 'Advantage' ELSE jmm.application_code END) fulfilment_system\n" +
                      "         , jmm.printer_location_type warehouse_primary\n" +
                      "         , jmm.back_stock_warehouse_location_type warehouse_backissue\n" +
                      "         , jmw.issues_per_volume_quantity number_of_issues\n" +
                      "         , null pod_supplier\n" +
                      "         , jmw.ddp_eligible_ind ddp_eligible\n" +
                      "         , jmw.pts_journal_indicator pts_journal_indicator\n" +
                      "         , jmw.year_of_first_issue year_of_first_issue\n" +
                      "         , jmw.first_issue_name first_issue_name\n" +
                      "         , jmw.first_volume_name first_volume_name\n" +
                      "         , (CASE WHEN (w.f_subscription_type IS NULL) THEN 'RY' ELSE w.f_subscription_type END) default_start\n" +
                      "         , 'TBC' short_title\n" +
                      "         , fa.f_gl_cost_resp_centre rc_code\n" +
                      "         , w.f_pmc pmc_code\n" +
                      "         FROM\n" +
                      "           ((((((((((("+GetJMDLDBUser.getProdDataBase()+".gd_product p\n" +
                      "         LEFT JOIN "+GetJMDLDBUser.getProdDataBase()+".gd_manifestation m ON (p.f_manifestation = m.manifestation_id))\n" +
                      "         LEFT JOIN "+GetJMDLDBUser.getProdDataBase()+".gd_wwork w ON ((CASE WHEN (p.f_manifestation IS NULL) THEN p.f_wwork ELSE m.f_wwork END) = w.work_id))\n" +
                      "         LEFT JOIN "+GetJMDLDBUser.getProdDataBase()+".gd_x_lov_work_type wt ON (w.f_type = wt.code))\n" +
                      "         LEFT JOIN "+GetJMDLDBUser.getProdDataBase()+".gd_x_lov_work_status ws ON (w.f_status = ws.code))\n" +
                      "         LEFT JOIN "+GetJMDLDBUser.getProdDataBase()+".gd_x_lov_product_status ps ON (p.f_status = ps.code))\n" +
                      "         LEFT JOIN "+GetJMDLDBUser.getProdDataBase()+".gd_work_identifier w_id1 ON (((w_id1.f_wwork = w.work_id) AND (w_id1.f_type = 'ELSEVIER JOURNAL NUMBER')) AND (w_id1.effective_end_date IS NULL)))\n" +
                      "         LEFT JOIN "+GetJMDLDBUser.getProdDataBase()+".gd_work_identifier w_id2 ON (((w_id2.f_wwork = w.work_id) AND (w_id2.f_type = 'JOURNAL ACRONYM')) AND (w_id2.effective_end_date IS NULL)))\n" +
                      "         LEFT JOIN "+GetJMDLDBUser.getProdDataBase()+".gd_work_financial_attribs fa ON ((fa.f_wwork = w.work_id) AND (fa.effective_end_date IS NULL)))\n" +
                      "         LEFT JOIN "+GetJMDLDBUser.getProdDataBase()+".gd_manifestation_identifier m_id ON (((m_id.f_manifestation = m.manifestation_id) AND (m_id.f_type = 'ISSN')) AND (m_id.effective_end_date IS NULL)))\n" +
                      "         LEFT JOIN (\n" +
                      "            SELECT work.*\n" +
                      "            FROM\n" +
                      "              (("+GetJMDLDBUser.getJMDB()+".jmf_work work\n" +
                      "            INNER JOIN "+GetJMDLDBUser.getJMDB()+".jmf_work_chronicle chronicle ON (chronicle.work_chronicle_id = work.work_chronicle_id))\n" +
                      "            INNER JOIN "+GetJMDLDBUser.getJMDB()+".jmf_chronicle_scenario scenario ON (scenario.chronicle_scenario_code = chronicle.chronicle_scenario_code))\n" +
                      "            WHERE ((scenario.chronicle_scenario_evolutionary_ind = 'N') AND (work.work_journey_identifier = 'A1'))\n" +
                      "         )  jmw ON ((w_id2.identifier = jmw.journal_acronym_pts) AND ((w_id2.f_type = 'JOURNAL ACRONYM') AND (w_id2.effective_end_date IS NULL))))\n" +
                      "         LEFT JOIN "+GetJMDLDBUser.getJMDB()+".jmf_manifestation jmm ON ((((jmm.issn = m_id.identifier) AND (m_id.f_type = 'ISSN')) AND (m_id.effective_end_date IS NULL)) AND (jmm.f_work = jmw.work_id)))\n" +
                      "         WHERE ((wt.roll_up_type = 'Journal') AND (p.f_type IN ('PPV', 'SUB')))\n" +
                      "      )  jmnew\n" +
                      "      WHERE (jmnew.fulfilment_system IN ('AMPS', 'QSS', 'Argi', 'Advantage'))\n" +
                      "   ) UNION    SELECT *\n" +
                      "   FROM\n" +
                      "     (\n" +
                      "      SELECT DISTINCT\n" +
                      "        '3.1' schemaversion\n" +
                      "      , least(COALESCE(p.b_credate, date_parse('9999-01-01 00:00:00.000', '%Y-%m-%d %H:%i:%s.%f')), least(COALESCE(w.b_credate, date_parse('9999-01-01 00:00:00.000', '%Y-%m-%d %H:%i:%s.%f')), least(COALESCE(m.b_credate, date_parse('9999-01-01 00:00:00.000', '%Y-%m-%d %H:%i:%s.%f')), least(COALESCE(w_id1.b_credate, date_parse('9999-01-01 00:00:00.000', '%Y-%m-%d %H:%i:%s.%f')), least(COALESCE(w_id2.b_credate, date_parse('9999-01-01 00:00:00.000', '%Y-%m-%d %H:%i:%s.%f')), least(COALESCE(m_id.b_credate, date_parse('9999-01-01 00:00:00.000', '%Y-%m-%d %H:%i:%s.%f')), COALESCE(fa.b_credate, date_parse('9999-01-01 00:00:00.000', '%Y-%m-%d %H:%i:%s.%f')))))))) created\n" +
                      "      , greatest(COALESCE(p.b_upddate, date_parse('1900-01-01 00:00:00.000', '%Y-%m-%d %H:%i:%s.%f')), greatest(COALESCE(w.b_upddate, date_parse('1900-01-01 00:00:00.000', '%Y-%m-%d %H:%i:%s.%f')), greatest(COALESCE(m.b_upddate, date_parse('1900-01-01 00:00:00.000', '%Y-%m-%d %H:%i:%s.%f')), greatest(COALESCE(w_id1.b_upddate, date_parse('1900-01-01 00:00:00.000', '%Y-%m-%d %H:%i:%s.%f')), greatest(COALESCE(w_id2.b_upddate, date_parse('1900-01-01 00:00:00.000', '%Y-%m-%d %H:%i:%s.%f')), greatest(COALESCE(m_id.b_upddate, date_parse('1900-01-01 00:00:00.000', '%Y-%m-%d %H:%i:%s.%f')), COALESCE(fa.b_upddate, date_parse('1900-01-01 00:00:00.000', '%Y-%m-%d %H:%i:%s.%f')))))))) updated\n" +
                      "      , p.product_id product_id\n" +
                      "      , p.name product_full_name\n" +
                      "      , p.short_name product_name\n" +
                      "      , w.work_title work_title\n" +
                      "      , wt.l_description work_type\n" +
                      "      , wt.roll_up_type work_typerollup\n" +
                      "      , ws.l_description work_status\n" +
                      "      , ws.roll_up_status work_statusrollup\n" +
                      "      , m.manifestation_id manifestation_id\n" +
                      "      , w_id1.identifier journal_number\n" +
                      "      , w_id2.identifier journal_acronym\n" +
                      "      , fa.f_gl_company operating_company\n" +
                      "      , (CASE WHEN (p.f_type = 'PPV') THEN 'PPV' WHEN (m.f_type = 'JPR') THEN 'Print' WHEN (m.f_type = 'JEL') THEN 'Online' ELSE 'Unknown' END) product_type\n" +
                      "      , m_id.identifier issn\n" +
                      "      , ps.roll_up_status publication_status\n" +
                      "      , p.f_tax_code etax_code\n" +
                      "      , 'Advantage' fulfilment_system\n" +
                      "      , jmnew.printer_location_type warehouse_primary\n" +
                      "      , jmnew.back_stock_warehouse_location_type warehouse_backissue\n" +
                      "      , jmnew.issues_per_volume_quantity number_of_issues\n" +
                      "      , null pod_supplier\n" +
                      "      , jmnew.ddp_eligible_ind ddp_eligible\n" +
                      "      , COALESCE(jmupd.pts_journal_indicator, jmnew.pts_journal_indicator) pts_journal_indicator\n" +
                      "      , COALESCE(jmupd.year_of_first_issue, jmnew.year_of_first_issue) year_of_first_issue\n" +
                      "      , COALESCE(jmupd.first_issue_name, jmnew.first_issue_name) first_issue_name\n" +
                      "      , COALESCE(jmupd.first_volume_name, jmnew.first_volume_name) first_volume_name\n" +
                      "      , (CASE WHEN (w.f_subscription_type IS NULL) THEN 'RY' ELSE w.f_subscription_type END) default_start\n" +
                      "      , 'TBC' short_title\n" +
                      "      , fa.f_gl_cost_resp_centre rc_code\n" +
                      "      , w.f_pmc pmc_code\n" +
                      "      FROM\n" +
                      "        ((((((((((("+GetJMDLDBUser.getProdDataBase()+".gd_product p\n" +
                      "      LEFT JOIN "+GetJMDLDBUser.getProdDataBase()+".gd_manifestation m ON (p.f_manifestation = m.manifestation_id))\n" +
                      "      LEFT JOIN "+GetJMDLDBUser.getProdDataBase()+".gd_wwork w ON ((CASE WHEN (p.f_manifestation IS NULL) THEN p.f_wwork ELSE m.f_wwork END) = w.work_id))\n" +
                      "      LEFT JOIN "+GetJMDLDBUser.getProdDataBase()+".gd_x_lov_work_type wt ON (w.f_type = wt.code))\n" +
                      "      LEFT JOIN "+GetJMDLDBUser.getProdDataBase()+".gd_x_lov_work_status ws ON (w.f_status = ws.code))\n" +
                      "      LEFT JOIN "+GetJMDLDBUser.getProdDataBase()+".gd_x_lov_product_status ps ON (p.f_status = ps.code))\n" +
                      "      LEFT JOIN "+GetJMDLDBUser.getProdDataBase()+".gd_work_identifier w_id1 ON (((w_id1.f_wwork = w.work_id) AND (w_id1.f_type = 'ELSEVIER JOURNAL NUMBER')) AND (w_id1.effective_end_date IS NULL)))\n" +
                      "      LEFT JOIN "+GetJMDLDBUser.getProdDataBase()+".gd_work_identifier w_id2 ON (((w_id2.f_wwork = w.work_id) AND (w_id2.f_type = 'JOURNAL ACRONYM')) AND (w_id2.effective_end_date IS NULL)))\n" +
                      "      LEFT JOIN "+GetJMDLDBUser.getProdDataBase()+".gd_work_financial_attribs fa ON ((fa.f_wwork = w.work_id) AND (fa.effective_end_date IS NULL)))\n" +
                      "      LEFT JOIN "+GetJMDLDBUser.getProdDataBase()+".gd_manifestation_identifier m_id ON (((m_id.f_manifestation = m.manifestation_id) AND (m_id.f_type = 'ISSN')) AND (m_id.effective_end_date IS NULL)))\n" +
                      "      LEFT JOIN (\n" +
                      "         SELECT\n" +
                      "           work.elsevier_journal_number\n" +
                      "         , manifestation.issn\n" +
                      "         , manifestation.application_code\n" +
                      "         , manifestation.printer_location_type\n" +
                      "         , manifestation.back_stock_warehouse_location_type\n" +
                      "         , work.issues_per_volume_quantity\n" +
                      "         , work.ddp_eligible_ind\n" +
                      "         , work.pts_journal_indicator\n" +
                      "         , work.year_of_first_issue\n" +
                      "         , work.first_issue_name\n" +
                      "         , work.first_volume_name\n" +
                      "         FROM\n" +
                      "           ((("+GetJMDLDBUser.getJMDB()+".jmf_work work\n" +
                      "         INNER JOIN "+GetJMDLDBUser.getJMDB()+".jmf_work_chronicle chronicle ON (chronicle.work_chronicle_id = work.work_chronicle_id))\n" +
                      "         INNER JOIN "+GetJMDLDBUser.getJMDB()+".jmf_chronicle_scenario scenario ON (scenario.chronicle_scenario_code = chronicle.chronicle_scenario_code))\n" +
                      "         INNER JOIN "+GetJMDLDBUser.getJMDB()+".jmf_manifestation manifestation ON (manifestation.f_work = work.work_id))\n" +
                      "         WHERE (((scenario.chronicle_scenario_evolutionary_ind = 'N') AND (work.work_journey_identifier = 'A1')) AND (manifestation.application_code IN ('AMPS', 'QSS', 'Argi', 'Advantage')))\n" +
                      "      )  jmnew ON ((jmnew.elsevier_journal_number = w_id1.identifier) AND (jmnew.issn = m_id.identifier)))\n" +
                      "      LEFT JOIN (\n" +
                      "         SELECT\n" +
                      "           work.elsevier_journal_number\n" +
                      "         , work.pts_journal_indicator\n" +
                      "         , work.year_of_first_issue\n" +
                      "         , work.first_issue_name\n" +
                      "         , work.first_volume_name\n" +
                      "         FROM\n" +
                      "           ("+GetJMDLDBUser.getJMDB()+".jmf_work work\n" +
                      "         INNER JOIN "+GetJMDLDBUser.getJMDB()+".jmf_work_chronicle chronicle ON (chronicle.work_chronicle_id = work.work_chronicle_id))\n" +
                      "         WHERE (((chronicle.chronicle_scenario_code = 'RN') AND (work.work_journey_identifier = 'A1')) AND (chronicle.notified_date = (SELECT max(chronicle2.notified_date)\n" +
                      "FROM\n" +
                      "  ("+GetJMDLDBUser.getJMDB()+".jmf_work work2\n" +
                      "INNER JOIN "+GetJMDLDBUser.getJMDB()+".jmf_work_chronicle chronicle2 ON (chronicle2.work_chronicle_id = work2.work_chronicle_id))\n" +
                      "WHERE ((chronicle2.chronicle_scenario_code = chronicle.chronicle_scenario_code) AND (work2.elsevier_journal_number = work.elsevier_journal_number))\n" +
                      ")))\n" +
                      "      )  jmupd ON (jmupd.elsevier_journal_number = w_id1.identifier))\n" +
                      "      WHERE (((wt.roll_up_type = 'Journal') AND (p.f_type = 'PPV')) AND ((jmnew.elsevier_journal_number IS NOT NULL) OR (jmupd.elsevier_journal_number IS NOT NULL)))\n" +
                      "   ) \n" +
                      ") \n" +
                      ")\n";

     public static String GET_MERCURY_PRINT_COUNT=
             "select count(*) as Target_count from "+GetPRMDLDBUser.getProdDataBase()+".extract_mercury_print_v";

    public static String GET_RANDOM_ID_MERCURY =
            "select product_id as randomIds from(\n" +
                    "SELECT *\n" +
                    "FROM\n" +
                    "  (\n" +
                    "   SELECT DISTINCT\n" +
                    "     '3.1' schemaversion\n" +
                    "   , least(COALESCE(p.b_credate, date_parse('9999-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')), least(COALESCE(w.b_credate, date_parse('9999-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')), least(COALESCE(m.b_credate, date_parse('9999-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')), least(COALESCE(w_id1.b_credate, date_parse('9999-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')), least(COALESCE(w_id2.b_credate, date_parse('9999-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')), least(COALESCE(m_id.b_credate, date_parse('9999-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')), COALESCE(fa.b_credate, date_parse('9999-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')))))))) created\n" +
                    "   , greatest(COALESCE(p.b_upddate, date_parse('1900-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')), greatest(COALESCE(w.b_upddate, date_parse('1900-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')), greatest(COALESCE(m.b_upddate, date_parse('1900-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')), greatest(COALESCE(w_id1.b_upddate, date_parse('1900-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')), greatest(COALESCE(w_id2.b_upddate, date_parse('1900-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')), greatest(COALESCE(m_id.b_upddate, date_parse('1900-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')), COALESCE(fa.b_upddate, date_parse('1900-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')))))))) updated\n" +
                    "   , p.product_id product_id\n" +
                    "   , p.name product_full_name\n" +
                    "   , p.short_name product_name\n" +
                    "   , w.work_title work_title\n" +
                    "   , wt.l_description work_type\n" +
                    "   , wt.roll_up_type work_typerollup\n" +
                    "   , ws.l_description work_status\n" +
                    "   , ws.roll_up_status work_statusrollup\n" +
                    "   , m.manifestation_id manifestation_id\n" +
                    "   , w_id1.identifier journal_number\n" +
                    "   , w_id2.identifier journal_acronym\n" +
                    "   , fa.f_gl_company operating_company\n" +
                    "   , (CASE WHEN (p.f_type = 'PPV') THEN 'PPV' WHEN (m.f_type = 'JPR') THEN 'Print' WHEN (m.f_type = 'JEL') THEN 'Online' ELSE 'Unknown' END) product_type\n" +
                    "   , m_id.identifier issn\n" +
                    "   , ps.roll_up_status publication_status\n" +
                    "   , p.f_tax_code etax_code\n" +
                    "   , mpa.fulfilment_system fulfilment_system\n" +
                    "   , mpa.warehouse_primary warehouse_primary\n" +
                    "   , mpa.warehouse_backissue warehouse_backissue\n" +
                    "   , mpa.number_of_issues number_of_issues\n" +
                    "   , mpa.pod_supplier pod_supplier\n" +
                    "   , mpa.ddp_eligible ddp_eligible\n" +
                    "   , COALESCE(jmupd.pts_journal_indicator, mpa.pts_journal_indicator) pts_journal_indicator\n" +
                    "   , jmupd.year_of_first_issue year_of_first_issue\n" +
                    "   , jmupd.first_issue_name first_issue_name\n" +
                    "   , jmupd.first_volume_name first_volume_name\n" +
                    "   , (CASE WHEN (w.f_subscription_type IS NULL) THEN 'RY' ELSE w.f_subscription_type END) default_start\n" +
                    "   , 'TBC' short_title\n" +
                    "   , fa.f_gl_cost_resp_centre rc_code\n" +
                    "   , w.f_pmc pmc_code\n" +
                    "   FROM\n" +
                    "     ((((((((((("+GetPRMDLDBUser.getProdDataBase()+".gd_product p\n" +
                    "   LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_manifestation m ON (p.f_manifestation = m.manifestation_id))\n" +
                    "   LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_wwork w ON ((CASE WHEN (p.f_manifestation IS NULL) THEN p.f_wwork ELSE m.f_wwork END) = w.work_id))\n" +
                    "   LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_work_type wt ON (w.f_type = wt.code))\n" +
                    "   LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_work_status ws ON (w.f_status = ws.code))\n" +
                    "   LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_product_status ps ON (p.f_status = ps.code))\n" +
                    "   LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_work_identifier w_id1 ON ((w_id1.f_wwork = w.work_id) AND ((w_id1.f_type = 'ELSEVIER JOURNAL NUMBER') AND (w_id1.effective_end_date IS NULL))))\n" +
                    "   LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_work_identifier w_id2 ON ((w_id2.f_wwork = w.work_id) AND ((w_id2.f_type = 'JOURNAL ACRONYM') AND (w_id2.effective_end_date IS NULL))))\n" +
                    "   LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_work_financial_attribs fa ON ((fa.f_wwork = w.work_id) AND (fa.effective_end_date IS NULL)))\n" +
                    "   LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_manifestation_identifier m_id ON (((m_id.f_manifestation = m.manifestation_id) AND (m_id.f_type = 'ISSN')) AND (m_id.effective_end_date IS NULL)))\n" +
                    "   INNER JOIN "+GetPRMDLDBUser.getProdDataBase()+".mercury_print_addl mpa ON (mpa.product_id = p.product_id))\n" +
                    "   LEFT JOIN (\n" +
                    "      SELECT\n" +
                    "        work.elsevier_journal_number\n" +
                    "      , work.pts_journal_indicator\n" +
                    "      , work.year_of_first_issue\n" +
                    "      , work.first_issue_name\n" +
                    "      , work.first_volume_name\n" +
                    "      FROM\n" +
                    "        ("+GetJMDLDBUser.getJMDB()+".jmf_work work\n" +
                    "      INNER JOIN "+GetJMDLDBUser.getJMDB()+".jmf_work_chronicle chronicle ON (chronicle.work_chronicle_id = work.work_chronicle_id))\n" +
                    "      WHERE (((chronicle.chronicle_scenario_code = 'RN') AND (work.work_journey_identifier = 'A1')) AND (chronicle.notified_date = (SELECT max(chronicle2.notified_date)\n" +
                    "FROM\n" +
                    "  ("+GetJMDLDBUser.getJMDB()+".jmf_work work2\n" +
                    "INNER JOIN "+GetJMDLDBUser.getJMDB()+".jmf_work_chronicle chronicle2 ON (chronicle2.work_chronicle_id = work2.work_chronicle_id))\n" +
                    "WHERE ((chronicle2.chronicle_scenario_code = chronicle.chronicle_scenario_code) AND (work2.elsevier_journal_number = work.elsevier_journal_number))\n" +
                    ")))\n" +
                    "   )  jmupd ON (jmupd.elsevier_journal_number = w_id1.identifier))\n" +
                    "   WHERE ((wt.roll_up_type = 'Journal') AND (p.f_type IN ('PPV', 'SUB')))\n" +
                    "UNION (\n" +
                    "      SELECT *\n" +
                    "      FROM\n" +
                    "        (\n" +
                    "         SELECT DISTINCT\n" +
                    "           '3.1' schemaversion\n" +
                    "         , least(COALESCE(p.b_credate, date_parse('9999-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')), least(COALESCE(w.b_credate, date_parse('9999-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')), least(COALESCE(m.b_credate, date_parse('9999-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')), least(COALESCE(w_id1.b_credate, date_parse('9999-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')), least(COALESCE(w_id2.b_credate, date_parse('9999-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')), least(COALESCE(m_id.b_credate, date_parse('9999-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')), COALESCE(fa.b_credate, date_parse('9999-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')))))))) created\n" +
                    "         , greatest(COALESCE(p.b_upddate, date_parse('1900-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')), greatest(COALESCE(w.b_upddate, date_parse('1900-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')), greatest(COALESCE(m.b_upddate, date_parse('1900-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')), greatest(COALESCE(w_id1.b_upddate, date_parse('1900-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')), greatest(COALESCE(w_id2.b_upddate, date_parse('1900-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')), greatest(COALESCE(m_id.b_upddate, date_parse('1900-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')), COALESCE(fa.b_upddate, date_parse('1900-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')))))))) updated\n" +
                    "         , p.product_id product_id\n" +
                    "         , p.name product_full_name\n" +
                    "         , p.short_name product_name\n" +
                    "         , w.work_title work_title\n" +
                    "         , wt.l_description work_type\n" +
                    "         , wt.roll_up_type work_typerollup\n" +
                    "         , ws.l_description work_status\n" +
                    "         , ws.roll_up_status work_statusrollup\n" +
                    "         , m.manifestation_id manifestation_id\n" +
                    "         , w_id1.identifier journal_number\n" +
                    "         , w_id2.identifier journal_acronym\n" +
                    "         , fa.f_gl_company operating_company\n" +
                    "         , (CASE WHEN (p.f_type = 'PPV') THEN 'PPV' WHEN (m.f_type = 'JPR') THEN 'Print' WHEN (m.f_type = 'JEL') THEN 'Online' ELSE 'Unknown' END) product_type\n" +
                    "         , m_id.identifier issn\n" +
                    "         , ps.roll_up_status publication_status\n" +
                    "         , p.f_tax_code etax_code\n" +
                    "         , (CASE WHEN (jmm.application_code IN ('AMPS', 'QSS', 'Argi')) THEN 'Advantage' ELSE jmm.application_code END) fulfilment_system\n" +
                    "         , jmm.printer_location_type warehouse_primary\n" +
                    "         , jmm.back_stock_warehouse_location_type warehouse_backissue\n" +
                    "         , jmw.issues_per_volume_quantity number_of_issues\n" +
                    "         , null pod_supplier\n" +
                    "         , jmw.ddp_eligible_ind ddp_eligible\n" +
                    "         , jmw.pts_journal_indicator pts_journal_indicator\n" +
                    "         , jmw.year_of_first_issue year_of_first_issue\n" +
                    "         , jmw.first_issue_name first_issue_name\n" +
                    "         , jmw.first_volume_name first_volume_name\n" +
                    "         , (CASE WHEN (w.f_subscription_type IS NULL) THEN 'RY' ELSE w.f_subscription_type END) default_start\n" +
                    "         , 'TBC' short_title\n" +
                    "         , fa.f_gl_cost_resp_centre rc_code\n" +
                    "         , w.f_pmc pmc_code\n" +
                    "         FROM\n" +
                    "           ((((((((((("+GetJMDLDBUser.getProdDataBase()+".gd_product p\n" +
                    "         LEFT JOIN "+GetJMDLDBUser.getProdDataBase()+".gd_manifestation m ON (p.f_manifestation = m.manifestation_id))\n" +
                    "         LEFT JOIN "+GetJMDLDBUser.getProdDataBase()+".gd_wwork w ON ((CASE WHEN (p.f_manifestation IS NULL) THEN p.f_wwork ELSE m.f_wwork END) = w.work_id))\n" +
                    "         LEFT JOIN "+GetJMDLDBUser.getProdDataBase()+".gd_x_lov_work_type wt ON (w.f_type = wt.code))\n" +
                    "         LEFT JOIN "+GetJMDLDBUser.getProdDataBase()+".gd_x_lov_work_status ws ON (w.f_status = ws.code))\n" +
                    "         LEFT JOIN "+GetJMDLDBUser.getProdDataBase()+".gd_x_lov_product_status ps ON (p.f_status = ps.code))\n" +
                    "         LEFT JOIN "+GetJMDLDBUser.getProdDataBase()+".gd_work_identifier w_id1 ON (((w_id1.f_wwork = w.work_id) AND (w_id1.f_type = 'ELSEVIER JOURNAL NUMBER')) AND (w_id1.effective_end_date IS NULL)))\n" +
                    "         LEFT JOIN "+GetJMDLDBUser.getProdDataBase()+".gd_work_identifier w_id2 ON (((w_id2.f_wwork = w.work_id) AND (w_id2.f_type = 'JOURNAL ACRONYM')) AND (w_id2.effective_end_date IS NULL)))\n" +
                    "         LEFT JOIN "+GetJMDLDBUser.getProdDataBase()+".gd_work_financial_attribs fa ON ((fa.f_wwork = w.work_id) AND (fa.effective_end_date IS NULL)))\n" +
                    "         LEFT JOIN "+GetJMDLDBUser.getProdDataBase()+".gd_manifestation_identifier m_id ON (((m_id.f_manifestation = m.manifestation_id) AND (m_id.f_type = 'ISSN')) AND (m_id.effective_end_date IS NULL)))\n" +
                    "         LEFT JOIN (\n" +
                    "            SELECT work.*\n" +
                    "            FROM\n" +
                    "              (("+GetJMDLDBUser.getJMDB()+".jmf_work work\n" +
                    "            INNER JOIN "+GetJMDLDBUser.getJMDB()+".jmf_work_chronicle chronicle ON (chronicle.work_chronicle_id = work.work_chronicle_id))\n" +
                    "            INNER JOIN "+GetJMDLDBUser.getJMDB()+".jmf_chronicle_scenario scenario ON (scenario.chronicle_scenario_code = chronicle.chronicle_scenario_code))\n" +
                    "            WHERE ((scenario.chronicle_scenario_evolutionary_ind = 'N') AND (work.work_journey_identifier = 'A1'))\n" +
                    "         )  jmw ON ((w_id2.identifier = jmw.journal_acronym_pts) AND ((w_id2.f_type = 'JOURNAL ACRONYM') AND (w_id2.effective_end_date IS NULL))))\n" +
                    "         LEFT JOIN "+GetJMDLDBUser.getJMDB()+".jmf_manifestation jmm ON ((((jmm.issn = m_id.identifier) AND (m_id.f_type = 'ISSN')) AND (m_id.effective_end_date IS NULL)) AND (jmm.f_work = jmw.work_id)))\n" +
                    "         WHERE ((wt.roll_up_type = 'Journal') AND (p.f_type IN ('PPV', 'SUB')))\n" +
                    "      )  jmnew\n" +
                    "      WHERE (jmnew.fulfilment_system IN ('AMPS', 'QSS', 'Argi', 'Advantage'))\n" +
                    "   ) UNION    SELECT *\n" +
                    "   FROM\n" +
                    "     (\n" +
                    "      SELECT DISTINCT\n" +
                    "        '3.1' schemaversion\n" +
                    "      , least(COALESCE(p.b_credate, date_parse('9999-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')), least(COALESCE(w.b_credate, date_parse('9999-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')), least(COALESCE(m.b_credate, date_parse('9999-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')), least(COALESCE(w_id1.b_credate, date_parse('9999-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')), least(COALESCE(w_id2.b_credate, date_parse('9999-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')), least(COALESCE(m_id.b_credate, date_parse('9999-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')), COALESCE(fa.b_credate, date_parse('9999-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')))))))) created\n" +
                    "      , greatest(COALESCE(p.b_upddate, date_parse('1900-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')), greatest(COALESCE(w.b_upddate, date_parse('1900-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')), greatest(COALESCE(m.b_upddate, date_parse('1900-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')), greatest(COALESCE(w_id1.b_upddate, date_parse('1900-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')), greatest(COALESCE(w_id2.b_upddate, date_parse('1900-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')), greatest(COALESCE(m_id.b_upddate, date_parse('1900-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')), COALESCE(fa.b_upddate, date_parse('1900-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')))))))) updated\n" +
                    "      , p.product_id product_id\n" +
                    "      , p.name product_full_name\n" +
                    "      , p.short_name product_name\n" +
                    "      , w.work_title work_title\n" +
                    "      , wt.l_description work_type\n" +
                    "      , wt.roll_up_type work_typerollup\n" +
                    "      , ws.l_description work_status\n" +
                    "      , ws.roll_up_status work_statusrollup\n" +
                    "      , m.manifestation_id manifestation_id\n" +
                    "      , w_id1.identifier journal_number\n" +
                    "      , w_id2.identifier journal_acronym\n" +
                    "      , fa.f_gl_company operating_company\n" +
                    "      , (CASE WHEN (p.f_type = 'PPV') THEN 'PPV' WHEN (m.f_type = 'JPR') THEN 'Print' WHEN (m.f_type = 'JEL') THEN 'Online' ELSE 'Unknown' END) product_type\n" +
                    "      , m_id.identifier issn\n" +
                    "      , ps.roll_up_status publication_status\n" +
                    "      , p.f_tax_code etax_code\n" +
                    "      , 'Advantage' fulfilment_system\n" +
                    "      , jmnew.printer_location_type warehouse_primary\n" +
                    "      , jmnew.back_stock_warehouse_location_type warehouse_backissue\n" +
                    "      , jmnew.issues_per_volume_quantity number_of_issues\n" +
                    "      , null pod_supplier\n" +
                    "      , jmnew.ddp_eligible_ind ddp_eligible\n" +
                    "      , COALESCE(jmupd.pts_journal_indicator, jmnew.pts_journal_indicator) pts_journal_indicator\n" +
                    "      , COALESCE(jmupd.year_of_first_issue, jmnew.year_of_first_issue) year_of_first_issue\n" +
                    "      , COALESCE(jmupd.first_issue_name, jmnew.first_issue_name) first_issue_name\n" +
                    "      , COALESCE(jmupd.first_volume_name, jmnew.first_volume_name) first_volume_name\n" +
                    "      , (CASE WHEN (w.f_subscription_type IS NULL) THEN 'RY' ELSE w.f_subscription_type END) default_start\n" +
                    "      , 'TBC' short_title\n" +
                    "      , fa.f_gl_cost_resp_centre rc_code\n" +
                    "      , w.f_pmc pmc_code\n" +
                    "      FROM\n" +
                    "        ((((((((((("+GetJMDLDBUser.getProdDataBase()+".gd_product p\n" +
                    "      LEFT JOIN "+GetJMDLDBUser.getProdDataBase()+".gd_manifestation m ON (p.f_manifestation = m.manifestation_id))\n" +
                    "      LEFT JOIN "+GetJMDLDBUser.getProdDataBase()+".gd_wwork w ON ((CASE WHEN (p.f_manifestation IS NULL) THEN p.f_wwork ELSE m.f_wwork END) = w.work_id))\n" +
                    "      LEFT JOIN "+GetJMDLDBUser.getProdDataBase()+".gd_x_lov_work_type wt ON (w.f_type = wt.code))\n" +
                    "      LEFT JOIN "+GetJMDLDBUser.getProdDataBase()+".gd_x_lov_work_status ws ON (w.f_status = ws.code))\n" +
                    "      LEFT JOIN "+GetJMDLDBUser.getProdDataBase()+".gd_x_lov_product_status ps ON (p.f_status = ps.code))\n" +
                    "      LEFT JOIN "+GetJMDLDBUser.getProdDataBase()+".gd_work_identifier w_id1 ON (((w_id1.f_wwork = w.work_id) AND (w_id1.f_type = 'ELSEVIER JOURNAL NUMBER')) AND (w_id1.effective_end_date IS NULL)))\n" +
                    "      LEFT JOIN "+GetJMDLDBUser.getProdDataBase()+".gd_work_identifier w_id2 ON (((w_id2.f_wwork = w.work_id) AND (w_id2.f_type = 'JOURNAL ACRONYM')) AND (w_id2.effective_end_date IS NULL)))\n" +
                    "      LEFT JOIN "+GetJMDLDBUser.getProdDataBase()+".gd_work_financial_attribs fa ON ((fa.f_wwork = w.work_id) AND (fa.effective_end_date IS NULL)))\n" +
                    "      LEFT JOIN "+GetJMDLDBUser.getProdDataBase()+".gd_manifestation_identifier m_id ON (((m_id.f_manifestation = m.manifestation_id) AND (m_id.f_type = 'ISSN')) AND (m_id.effective_end_date IS NULL)))\n" +
                    "      LEFT JOIN (\n" +
                    "         SELECT\n" +
                    "           work.elsevier_journal_number\n" +
                    "         , manifestation.issn\n" +
                    "         , manifestation.application_code\n" +
                    "         , manifestation.printer_location_type\n" +
                    "         , manifestation.back_stock_warehouse_location_type\n" +
                    "         , work.issues_per_volume_quantity\n" +
                    "         , work.ddp_eligible_ind\n" +
                    "         , work.pts_journal_indicator\n" +
                    "         , work.year_of_first_issue\n" +
                    "         , work.first_issue_name\n" +
                    "         , work.first_volume_name\n" +
                    "         FROM\n" +
                    "           ((("+GetJMDLDBUser.getJMDB()+".jmf_work work\n" +
                    "         INNER JOIN "+GetJMDLDBUser.getJMDB()+".jmf_work_chronicle chronicle ON (chronicle.work_chronicle_id = work.work_chronicle_id))\n" +
                    "         INNER JOIN "+GetJMDLDBUser.getJMDB()+".jmf_chronicle_scenario scenario ON (scenario.chronicle_scenario_code = chronicle.chronicle_scenario_code))\n" +
                    "         INNER JOIN "+GetJMDLDBUser.getJMDB()+".jmf_manifestation manifestation ON (manifestation.f_work = work.work_id))\n" +
                    "         WHERE (((scenario.chronicle_scenario_evolutionary_ind = 'N') AND (work.work_journey_identifier = 'A1')) AND (manifestation.application_code IN ('AMPS', 'QSS', 'Argi', 'Advantage')))\n" +
                    "      )  jmnew ON ((jmnew.elsevier_journal_number = w_id1.identifier) AND (jmnew.issn = m_id.identifier)))\n" +
                    "      LEFT JOIN (\n" +
                    "         SELECT\n" +
                    "           work.elsevier_journal_number\n" +
                    "         , work.pts_journal_indicator\n" +
                    "         , work.year_of_first_issue\n" +
                    "         , work.first_issue_name\n" +
                    "         , work.first_volume_name\n" +
                    "         FROM\n" +
                    "           ("+GetJMDLDBUser.getJMDB()+".jmf_work work\n" +
                    "         INNER JOIN "+GetJMDLDBUser.getJMDB()+".jmf_work_chronicle chronicle ON (chronicle.work_chronicle_id = work.work_chronicle_id))\n" +
                    "         WHERE (((chronicle.chronicle_scenario_code = 'RN') AND (work.work_journey_identifier = 'A1')) AND (chronicle.notified_date = (SELECT max(chronicle2.notified_date)\n" +
                    "FROM\n" +
                    "  ("+GetJMDLDBUser.getJMDB()+".jmf_work work2\n" +
                    "INNER JOIN "+GetJMDLDBUser.getJMDB()+".jmf_work_chronicle chronicle2 ON (chronicle2.work_chronicle_id = work2.work_chronicle_id))\n" +
                    "WHERE ((chronicle2.chronicle_scenario_code = chronicle.chronicle_scenario_code) AND (work2.elsevier_journal_number = work.elsevier_journal_number))\n" +
                    ")))\n" +
                    "      )  jmupd ON (jmupd.elsevier_journal_number = w_id1.identifier))\n" +
                    "      WHERE (((wt.roll_up_type = 'Journal') AND (p.f_type = 'PPV')) AND ((jmnew.elsevier_journal_number IS NOT NULL) OR (jmupd.elsevier_journal_number IS NOT NULL)))\n" +
                    "   ) \n" +
                    ") \n" +
                    ") order by rand() limit %s\n";

    public static String GET_EPH_SOURCE_DATA =
            "select * from(\n" +
                    "SELECT *\n" +
                    "FROM\n" +
                    "  (\n" +
                    "   SELECT DISTINCT\n" +
                    "     '3.1' schemaversion\n" +
                    "   , least(COALESCE(p.b_credate, date_parse('9999-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')), least(COALESCE(w.b_credate, date_parse('9999-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')), least(COALESCE(m.b_credate, date_parse('9999-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')), least(COALESCE(w_id1.b_credate, date_parse('9999-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')), least(COALESCE(w_id2.b_credate, date_parse('9999-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')), least(COALESCE(m_id.b_credate, date_parse('9999-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')), COALESCE(fa.b_credate, date_parse('9999-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')))))))) created\n" +
                    "   , greatest(COALESCE(p.b_upddate, date_parse('1900-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')), greatest(COALESCE(w.b_upddate, date_parse('1900-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')), greatest(COALESCE(m.b_upddate, date_parse('1900-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')), greatest(COALESCE(w_id1.b_upddate, date_parse('1900-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')), greatest(COALESCE(w_id2.b_upddate, date_parse('1900-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')), greatest(COALESCE(m_id.b_upddate, date_parse('1900-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')), COALESCE(fa.b_upddate, date_parse('1900-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')))))))) updated\n" +
                    "   , p.product_id product_id\n" +
                    "   , p.name product_full_name\n" +
                    "   , p.short_name product_name\n" +
                    "   , w.work_title work_title\n" +
                    "   , wt.l_description work_type\n" +
                    "   , wt.roll_up_type work_typerollup\n" +
                    "   , ws.l_description work_status\n" +
                    "   , ws.roll_up_status work_statusrollup\n" +
                    "   , m.manifestation_id manifestation_id\n" +
                    "   , w_id1.identifier journal_number\n" +
                    "   , w_id2.identifier journal_acronym\n" +
                    "   , fa.f_gl_company operating_company\n" +
                    "   , (CASE WHEN (p.f_type = 'PPV') THEN 'PPV' WHEN (m.f_type = 'JPR') THEN 'Print' WHEN (m.f_type = 'JEL') THEN 'Online' ELSE 'Unknown' END) product_type\n" +
                    "   , m_id.identifier issn\n" +
                    "   , ps.roll_up_status publication_status\n" +
                    "   , p.f_tax_code etax_code\n" +
                    "   , mpa.fulfilment_system fulfilment_system\n" +
                    "   , mpa.warehouse_primary warehouse_primary\n" +
                    "   , mpa.warehouse_backissue warehouse_backissue\n" +
                    "   , mpa.number_of_issues number_of_issues\n" +
                    "   , mpa.pod_supplier pod_supplier\n" +
                    "   , mpa.ddp_eligible ddp_eligible\n" +
                    "   , COALESCE(jmupd.pts_journal_indicator, mpa.pts_journal_indicator) pts_journal_indicator\n" +
                    "   , jmupd.year_of_first_issue year_of_first_issue\n" +
                    "   , jmupd.first_issue_name first_issue_name\n" +
                    "   , jmupd.first_volume_name first_volume_name\n" +
                    "   , (CASE WHEN (w.f_subscription_type IS NULL) THEN 'RY' ELSE w.f_subscription_type END) default_start\n" +
                    "   , 'TBC' short_title\n" +
                    "   , fa.f_gl_cost_resp_centre rc_code\n" +
                    "   , w.f_pmc pmc_code\n" +
                    "   FROM\n" +
                    "     ((((((((((("+GetPRMDLDBUser.getProdDataBase()+".gd_product p\n" +
                    "   LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_manifestation m ON (p.f_manifestation = m.manifestation_id))\n" +
                    "   LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_wwork w ON ((CASE WHEN (p.f_manifestation IS NULL) THEN p.f_wwork ELSE m.f_wwork END) = w.work_id))\n" +
                    "   LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_work_type wt ON (w.f_type = wt.code))\n" +
                    "   LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_work_status ws ON (w.f_status = ws.code))\n" +
                    "   LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_product_status ps ON (p.f_status = ps.code))\n" +
                    "   LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_work_identifier w_id1 ON ((w_id1.f_wwork = w.work_id) AND ((w_id1.f_type = 'ELSEVIER JOURNAL NUMBER') AND (w_id1.effective_end_date IS NULL))))\n" +
                    "   LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_work_identifier w_id2 ON ((w_id2.f_wwork = w.work_id) AND ((w_id2.f_type = 'JOURNAL ACRONYM') AND (w_id2.effective_end_date IS NULL))))\n" +
                    "   LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_work_financial_attribs fa ON ((fa.f_wwork = w.work_id) AND (fa.effective_end_date IS NULL)))\n" +
                    "   LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_manifestation_identifier m_id ON (((m_id.f_manifestation = m.manifestation_id) AND (m_id.f_type = 'ISSN')) AND (m_id.effective_end_date IS NULL)))\n" +
                    "   INNER JOIN "+GetPRMDLDBUser.getProdDataBase()+".mercury_print_addl mpa ON (mpa.product_id = p.product_id))\n" +
                    "   LEFT JOIN (\n" +
                    "      SELECT\n" +
                    "        work.elsevier_journal_number\n" +
                    "      , work.pts_journal_indicator\n" +
                    "      , work.year_of_first_issue\n" +
                    "      , work.first_issue_name\n" +
                    "      , work.first_volume_name\n" +
                    "      FROM\n" +
                    "        ("+GetJMDLDBUser.getJMDB()+".jmf_work work\n" +
                    "      INNER JOIN "+GetJMDLDBUser.getJMDB()+".jmf_work_chronicle chronicle ON (chronicle.work_chronicle_id = work.work_chronicle_id))\n" +
                    "      WHERE (((chronicle.chronicle_scenario_code = 'RN') AND (work.work_journey_identifier = 'A1')) AND (chronicle.notified_date = (SELECT max(chronicle2.notified_date)\n" +
                    "FROM\n" +
                    "  ("+GetJMDLDBUser.getJMDB()+".jmf_work work2\n" +
                    "INNER JOIN "+GetJMDLDBUser.getJMDB()+".jmf_work_chronicle chronicle2 ON (chronicle2.work_chronicle_id = work2.work_chronicle_id))\n" +
                    "WHERE ((chronicle2.chronicle_scenario_code = chronicle.chronicle_scenario_code) AND (work2.elsevier_journal_number = work.elsevier_journal_number))\n" +
                    ")))\n" +
                    "   )  jmupd ON (jmupd.elsevier_journal_number = w_id1.identifier))\n" +
                    "   WHERE ((wt.roll_up_type = 'Journal') AND (p.f_type IN ('PPV', 'SUB')))\n" +
                    "UNION (\n" +
                    "      SELECT *\n" +
                    "      FROM\n" +
                    "        (\n" +
                    "         SELECT DISTINCT\n" +
                    "           '3.1' schemaversion\n" +
                    "         , least(COALESCE(p.b_credate, date_parse('9999-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')), least(COALESCE(w.b_credate, date_parse('9999-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')), least(COALESCE(m.b_credate, date_parse('9999-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')), least(COALESCE(w_id1.b_credate, date_parse('9999-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')), least(COALESCE(w_id2.b_credate, date_parse('9999-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')), least(COALESCE(m_id.b_credate, date_parse('9999-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')), COALESCE(fa.b_credate, date_parse('9999-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')))))))) created\n" +
                    "         , greatest(COALESCE(p.b_upddate, date_parse('1900-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')), greatest(COALESCE(w.b_upddate, date_parse('1900-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')), greatest(COALESCE(m.b_upddate, date_parse('1900-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')), greatest(COALESCE(w_id1.b_upddate, date_parse('1900-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')), greatest(COALESCE(w_id2.b_upddate, date_parse('1900-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')), greatest(COALESCE(m_id.b_upddate, date_parse('1900-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')), COALESCE(fa.b_upddate, date_parse('1900-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')))))))) updated\n" +
                    "         , p.product_id product_id\n" +
                    "         , p.name product_full_name\n" +
                    "         , p.short_name product_name\n" +
                    "         , w.work_title work_title\n" +
                    "         , wt.l_description work_type\n" +
                    "         , wt.roll_up_type work_typerollup\n" +
                    "         , ws.l_description work_status\n" +
                    "         , ws.roll_up_status work_statusrollup\n" +
                    "         , m.manifestation_id manifestation_id\n" +
                    "         , w_id1.identifier journal_number\n" +
                    "         , w_id2.identifier journal_acronym\n" +
                    "         , fa.f_gl_company operating_company\n" +
                    "         , (CASE WHEN (p.f_type = 'PPV') THEN 'PPV' WHEN (m.f_type = 'JPR') THEN 'Print' WHEN (m.f_type = 'JEL') THEN 'Online' ELSE 'Unknown' END) product_type\n" +
                    "         , m_id.identifier issn\n" +
                    "         , ps.roll_up_status publication_status\n" +
                    "         , p.f_tax_code etax_code\n" +
                    "         , (CASE WHEN (jmm.application_code IN ('AMPS', 'QSS', 'Argi')) THEN 'Advantage' ELSE jmm.application_code END) fulfilment_system\n" +
                    "         , jmm.printer_location_type warehouse_primary\n" +
                    "         , jmm.back_stock_warehouse_location_type warehouse_backissue\n" +
                    "         , jmw.issues_per_volume_quantity number_of_issues\n" +
                    "         , null pod_supplier\n" +
                    "         , jmw.ddp_eligible_ind ddp_eligible\n" +
                    "         , jmw.pts_journal_indicator pts_journal_indicator\n" +
                    "         , jmw.year_of_first_issue year_of_first_issue\n" +
                    "         , jmw.first_issue_name first_issue_name\n" +
                    "         , jmw.first_volume_name first_volume_name\n" +
                    "         , (CASE WHEN (w.f_subscription_type IS NULL) THEN 'RY' ELSE w.f_subscription_type END) default_start\n" +
                    "         , 'TBC' short_title\n" +
                    "         , fa.f_gl_cost_resp_centre rc_code\n" +
                    "         , w.f_pmc pmc_code\n" +
                    "         FROM\n" +
                    "           ((((((((((("+GetJMDLDBUser.getProdDataBase()+".gd_product p\n" +
                    "         LEFT JOIN "+GetJMDLDBUser.getProdDataBase()+".gd_manifestation m ON (p.f_manifestation = m.manifestation_id))\n" +
                    "         LEFT JOIN "+GetJMDLDBUser.getProdDataBase()+".gd_wwork w ON ((CASE WHEN (p.f_manifestation IS NULL) THEN p.f_wwork ELSE m.f_wwork END) = w.work_id))\n" +
                    "         LEFT JOIN "+GetJMDLDBUser.getProdDataBase()+".gd_x_lov_work_type wt ON (w.f_type = wt.code))\n" +
                    "         LEFT JOIN "+GetJMDLDBUser.getProdDataBase()+".gd_x_lov_work_status ws ON (w.f_status = ws.code))\n" +
                    "         LEFT JOIN "+GetJMDLDBUser.getProdDataBase()+".gd_x_lov_product_status ps ON (p.f_status = ps.code))\n" +
                    "         LEFT JOIN "+GetJMDLDBUser.getProdDataBase()+".gd_work_identifier w_id1 ON (((w_id1.f_wwork = w.work_id) AND (w_id1.f_type = 'ELSEVIER JOURNAL NUMBER')) AND (w_id1.effective_end_date IS NULL)))\n" +
                    "         LEFT JOIN "+GetJMDLDBUser.getProdDataBase()+".gd_work_identifier w_id2 ON (((w_id2.f_wwork = w.work_id) AND (w_id2.f_type = 'JOURNAL ACRONYM')) AND (w_id2.effective_end_date IS NULL)))\n" +
                    "         LEFT JOIN "+GetJMDLDBUser.getProdDataBase()+".gd_work_financial_attribs fa ON ((fa.f_wwork = w.work_id) AND (fa.effective_end_date IS NULL)))\n" +
                    "         LEFT JOIN "+GetJMDLDBUser.getProdDataBase()+".gd_manifestation_identifier m_id ON (((m_id.f_manifestation = m.manifestation_id) AND (m_id.f_type = 'ISSN')) AND (m_id.effective_end_date IS NULL)))\n" +
                    "         LEFT JOIN (\n" +
                    "            SELECT work.*\n" +
                    "            FROM\n" +
                    "              (("+GetJMDLDBUser.getJMDB()+".jmf_work work\n" +
                    "            INNER JOIN "+GetJMDLDBUser.getJMDB()+".jmf_work_chronicle chronicle ON (chronicle.work_chronicle_id = work.work_chronicle_id))\n" +
                    "            INNER JOIN "+GetJMDLDBUser.getJMDB()+".jmf_chronicle_scenario scenario ON (scenario.chronicle_scenario_code = chronicle.chronicle_scenario_code))\n" +
                    "            WHERE ((scenario.chronicle_scenario_evolutionary_ind = 'N') AND (work.work_journey_identifier = 'A1'))\n" +
                    "         )  jmw ON ((w_id2.identifier = jmw.journal_acronym_pts) AND ((w_id2.f_type = 'JOURNAL ACRONYM') AND (w_id2.effective_end_date IS NULL))))\n" +
                    "         LEFT JOIN "+GetJMDLDBUser.getJMDB()+".jmf_manifestation jmm ON ((((jmm.issn = m_id.identifier) AND (m_id.f_type = 'ISSN')) AND (m_id.effective_end_date IS NULL)) AND (jmm.f_work = jmw.work_id)))\n" +
                    "         WHERE ((wt.roll_up_type = 'Journal') AND (p.f_type IN ('PPV', 'SUB')))\n" +
                    "      )  jmnew\n" +
                    "      WHERE (jmnew.fulfilment_system IN ('AMPS', 'QSS', 'Argi', 'Advantage'))\n" +
                    "   ) UNION    SELECT *\n" +
                    "   FROM\n" +
                    "     (\n" +
                    "      SELECT DISTINCT\n" +
                    "        '3.1' schemaversion\n" +
                    "      , least(COALESCE(p.b_credate, date_parse('9999-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')), least(COALESCE(w.b_credate, date_parse('9999-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')), least(COALESCE(m.b_credate, date_parse('9999-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')), least(COALESCE(w_id1.b_credate, date_parse('9999-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')), least(COALESCE(w_id2.b_credate, date_parse('9999-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')), least(COALESCE(m_id.b_credate, date_parse('9999-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')), COALESCE(fa.b_credate, date_parse('9999-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')))))))) created\n" +
                    "      , greatest(COALESCE(p.b_upddate, date_parse('1900-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')), greatest(COALESCE(w.b_upddate, date_parse('1900-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')), greatest(COALESCE(m.b_upddate, date_parse('1900-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')), greatest(COALESCE(w_id1.b_upddate, date_parse('1900-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')), greatest(COALESCE(w_id2.b_upddate, date_parse('1900-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')), greatest(COALESCE(m_id.b_upddate, date_parse('1900-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')), COALESCE(fa.b_upddate, date_parse('1900-01-01 00:00:00.000', '%%Y-%%m-%%d %%H:%%i:%%s.%%f')))))))) updated\n" +
                    "      , p.product_id product_id\n" +
                    "      , p.name product_full_name\n" +
                    "      , p.short_name product_name\n" +
                    "      , w.work_title work_title\n" +
                    "      , wt.l_description work_type\n" +
                    "      , wt.roll_up_type work_typerollup\n" +
                    "      , ws.l_description work_status\n" +
                    "      , ws.roll_up_status work_statusrollup\n" +
                    "      , m.manifestation_id manifestation_id\n" +
                    "      , w_id1.identifier journal_number\n" +
                    "      , w_id2.identifier journal_acronym\n" +
                    "      , fa.f_gl_company operating_company\n" +
                    "      , (CASE WHEN (p.f_type = 'PPV') THEN 'PPV' WHEN (m.f_type = 'JPR') THEN 'Print' WHEN (m.f_type = 'JEL') THEN 'Online' ELSE 'Unknown' END) product_type\n" +
                    "      , m_id.identifier issn\n" +
                    "      , ps.roll_up_status publication_status\n" +
                    "      , p.f_tax_code etax_code\n" +
                    "      , 'Advantage' fulfilment_system\n" +
                    "      , jmnew.printer_location_type warehouse_primary\n" +
                    "      , jmnew.back_stock_warehouse_location_type warehouse_backissue\n" +
                    "      , jmnew.issues_per_volume_quantity number_of_issues\n" +
                    "      , null pod_supplier\n" +
                    "      , jmnew.ddp_eligible_ind ddp_eligible\n" +
                    "      , COALESCE(jmupd.pts_journal_indicator, jmnew.pts_journal_indicator) pts_journal_indicator\n" +
                    "      , COALESCE(jmupd.year_of_first_issue, jmnew.year_of_first_issue) year_of_first_issue\n" +
                    "      , COALESCE(jmupd.first_issue_name, jmnew.first_issue_name) first_issue_name\n" +
                    "      , COALESCE(jmupd.first_volume_name, jmnew.first_volume_name) first_volume_name\n" +
                    "      , (CASE WHEN (w.f_subscription_type IS NULL) THEN 'RY' ELSE w.f_subscription_type END) default_start\n" +
                    "      , 'TBC' short_title\n" +
                    "      , fa.f_gl_cost_resp_centre rc_code\n" +
                    "      , w.f_pmc pmc_code\n" +
                    "      FROM\n" +
                    "        ((((((((((("+GetJMDLDBUser.getProdDataBase()+".gd_product p\n" +
                    "      LEFT JOIN "+GetJMDLDBUser.getProdDataBase()+".gd_manifestation m ON (p.f_manifestation = m.manifestation_id))\n" +
                    "      LEFT JOIN "+GetJMDLDBUser.getProdDataBase()+".gd_wwork w ON ((CASE WHEN (p.f_manifestation IS NULL) THEN p.f_wwork ELSE m.f_wwork END) = w.work_id))\n" +
                    "      LEFT JOIN "+GetJMDLDBUser.getProdDataBase()+".gd_x_lov_work_type wt ON (w.f_type = wt.code))\n" +
                    "      LEFT JOIN "+GetJMDLDBUser.getProdDataBase()+".gd_x_lov_work_status ws ON (w.f_status = ws.code))\n" +
                    "      LEFT JOIN "+GetJMDLDBUser.getProdDataBase()+".gd_x_lov_product_status ps ON (p.f_status = ps.code))\n" +
                    "      LEFT JOIN "+GetJMDLDBUser.getProdDataBase()+".gd_work_identifier w_id1 ON (((w_id1.f_wwork = w.work_id) AND (w_id1.f_type = 'ELSEVIER JOURNAL NUMBER')) AND (w_id1.effective_end_date IS NULL)))\n" +
                    "      LEFT JOIN "+GetJMDLDBUser.getProdDataBase()+".gd_work_identifier w_id2 ON (((w_id2.f_wwork = w.work_id) AND (w_id2.f_type = 'JOURNAL ACRONYM')) AND (w_id2.effective_end_date IS NULL)))\n" +
                    "      LEFT JOIN "+GetJMDLDBUser.getProdDataBase()+".gd_work_financial_attribs fa ON ((fa.f_wwork = w.work_id) AND (fa.effective_end_date IS NULL)))\n" +
                    "      LEFT JOIN "+GetJMDLDBUser.getProdDataBase()+".gd_manifestation_identifier m_id ON (((m_id.f_manifestation = m.manifestation_id) AND (m_id.f_type = 'ISSN')) AND (m_id.effective_end_date IS NULL)))\n" +
                    "      LEFT JOIN (\n" +
                    "         SELECT\n" +
                    "           work.elsevier_journal_number\n" +
                    "         , manifestation.issn\n" +
                    "         , manifestation.application_code\n" +
                    "         , manifestation.printer_location_type\n" +
                    "         , manifestation.back_stock_warehouse_location_type\n" +
                    "         , work.issues_per_volume_quantity\n" +
                    "         , work.ddp_eligible_ind\n" +
                    "         , work.pts_journal_indicator\n" +
                    "         , work.year_of_first_issue\n" +
                    "         , work.first_issue_name\n" +
                    "         , work.first_volume_name\n" +
                    "         FROM\n" +
                    "           ((("+GetJMDLDBUser.getJMDB()+".jmf_work work\n" +
                    "         INNER JOIN "+GetJMDLDBUser.getJMDB()+".jmf_work_chronicle chronicle ON (chronicle.work_chronicle_id = work.work_chronicle_id))\n" +
                    "         INNER JOIN "+GetJMDLDBUser.getJMDB()+".jmf_chronicle_scenario scenario ON (scenario.chronicle_scenario_code = chronicle.chronicle_scenario_code))\n" +
                    "         INNER JOIN "+GetJMDLDBUser.getJMDB()+".jmf_manifestation manifestation ON (manifestation.f_work = work.work_id))\n" +
                    "         WHERE (((scenario.chronicle_scenario_evolutionary_ind = 'N') AND (work.work_journey_identifier = 'A1')) AND (manifestation.application_code IN ('AMPS', 'QSS', 'Argi', 'Advantage')))\n" +
                    "      )  jmnew ON ((jmnew.elsevier_journal_number = w_id1.identifier) AND (jmnew.issn = m_id.identifier)))\n" +
                    "      LEFT JOIN (\n" +
                    "         SELECT\n" +
                    "           work.elsevier_journal_number\n" +
                    "         , work.pts_journal_indicator\n" +
                    "         , work.year_of_first_issue\n" +
                    "         , work.first_issue_name\n" +
                    "         , work.first_volume_name\n" +
                    "         FROM\n" +
                    "           ("+GetJMDLDBUser.getJMDB()+".jmf_work work\n" +
                    "         INNER JOIN "+GetJMDLDBUser.getJMDB()+".jmf_work_chronicle chronicle ON (chronicle.work_chronicle_id = work.work_chronicle_id))\n" +
                    "         WHERE (((chronicle.chronicle_scenario_code = 'RN') AND (work.work_journey_identifier = 'A1')) AND (chronicle.notified_date = (SELECT max(chronicle2.notified_date)\n" +
                    "FROM\n" +
                    "  ("+GetJMDLDBUser.getJMDB()+".jmf_work work2\n" +
                    "INNER JOIN "+GetJMDLDBUser.getJMDB()+".jmf_work_chronicle chronicle2 ON (chronicle2.work_chronicle_id = work2.work_chronicle_id))\n" +
                    "WHERE ((chronicle2.chronicle_scenario_code = chronicle.chronicle_scenario_code) AND (work2.elsevier_journal_number = work.elsevier_journal_number))\n" +
                    ")))\n" +
                    "      )  jmupd ON (jmupd.elsevier_journal_number = w_id1.identifier))\n" +
                    "      WHERE (((wt.roll_up_type = 'Journal') AND (p.f_type = 'PPV')) AND ((jmnew.elsevier_journal_number IS NOT NULL) OR (jmupd.elsevier_journal_number IS NOT NULL)))\n" +
                    "   ) \n" +
                    ") \n" +
                    ") where product_id in ('%s') order by product_id desc,issn desc, number_of_issues desc,year_of_first_issue desc,first_issue_name desc,first_volume_name desc\n";

    public static String GET_MERCURY_PRINT_RECS=
            "select * from "+GetPRMDLDBUser.getProdDataBase()+".extract_mercury_print_v where product_id in ('%s') order by product_id desc,issn desc," +
                    " number_of_issues desc,year_of_first_issue desc,first_issue_name desc,first_volume_name desc\n";

}




