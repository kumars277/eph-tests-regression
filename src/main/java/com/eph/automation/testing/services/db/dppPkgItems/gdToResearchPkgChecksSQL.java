package com.eph.automation.testing.services.db.dppPkgItems;

import com.eph.automation.testing.helper.Log;

public class gdToResearchPkgChecksSQL {
       public static final String GET_RANDOM_PROD_EPR_ID =
               "select epr_id from researchpackages.package_item order by random() limit %s";

       public static final String GET_SEMARCHY_RECS =
              "select p.product_id,\n" +
                      "case when mi.f_type='ISSN' then mi.identifier end as ISSN,\n" +
                      "case when wi.f_type='ELSEVIER JOURNAL NUMBER' then wi.identifier end as journal_number,\n" +
                      "null as ownership_type,\n" +
                      "pmc.f_pmg,\n" +
                      "case when wpr1.f_role='PU' then concat(per1.given_name, ' ' ,per1.family_name) end as publisher,\n" +
                      "case when wpr2.f_role='PD' then concat(per2.given_name, ' ',per2.family_name) end as publishing_director,\n" +
                      "w.work_title,\n" +
                      "w.f_legal_ownership as legal_ownership\n" +
                      "from semarchy_eph_mdm.gd_wwork w\n" +
                      "JOIN semarchy_eph_mdm.gd_manifestation m on w.work_id=m.f_wwork and m.f_type='JEL'\n" +
                      "join semarchy_eph_mdm.gd_manifestation_identifier mi on (m.manifestation_id=mi.f_manifestation and mi.f_type in ('ISSN'))\n" +
                      "join semarchy_eph_mdm.gd_work_identifier wi on (w.work_id=wi.f_wwork and wi.f_type in ('ELSEVIER JOURNAL NUMBER'))\n" +
                      "join semarchy_eph_mdm.gd_work_person_role wpr1 on w.work_id=wpr1.f_wwork and wpr1.f_role in ('PU')\n" +
                      "join semarchy_eph_mdm.gd_work_person_role wpr2 on w.work_id=wpr2.f_wwork and wpr2.f_role in ('PD')\n" +
                      "join semarchy_eph_mdm.gd_product p on (p.f_manifestation=m.manifestation_id or p.f_wwork = w.work_id)\n" +
                      "join semarchy_eph_mdm.gd_x_lov_pmc pmc on w.f_pmc=pmc.code\n" +
                      "join semarchy_eph_mdm.gd_person per1 on wpr1.f_person=per1.person_id\n" +
                      "join semarchy_eph_mdm.gd_person per2 on wpr2.f_person=per2.person_id\n" +
                      "where wpr1.effective_end_date is null and wpr2.effective_end_date is null and wi.effective_end_date is null and mi.effective_end_date is null\n" +
                      "and p.product_id in ('%s') order by p.product_id,journal_number\n";
    public static final String GET_RESEARCHPKG_PKGITEM_RECS =
           "select pkgi.epr_id, issn, journal_number, ownership_type, pmg_code, publisher,publishing_director,\n" +
                   "title, legal_ownership_type, \"version\"\n" +
                   "from researchpackages.package_item pkgi\n" +
                   "inner join (select epr_id, max(\"version\") max_ver from package_item group by epr_id) a on pkgi.epr_id=a.epr_id and a.max_ver=pkgi.\"version\"\n" +
                   "where pkgi.epr_id in ('%s') order by epr_id,journal_number";

}
