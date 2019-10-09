package com.eph.automation.testing.services.db.sql;

public class ResearchPackagesSQL {
    public static String SELECT_NEWLY_ADDED_JOURNAL_FROM_EPH_RESEARCH_PACKAGES = "select is_new_addition as \n"+
                                                " NEWLY_ADDED_JOURNAL_STATUS from researchpackages.package_item \n"+
                                                " where issn= '%s' order by last_update_time desc limit 1";
    public static String SELECT_RECENT_STATUS_UPDATED_JOURNAL_FROM_EPH_RESEARCH_PACKAGES = "select status as \n"+
                                                " JOURNAL_STATUS from researchpackages.package_item \n"+
                                                " where issn='%s' order by last_update_time desc limit 1";

}
