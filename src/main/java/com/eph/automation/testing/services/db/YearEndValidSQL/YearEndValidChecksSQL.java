package com.eph.automation.testing.services.db.YearEndValidSQL;

import com.eph.automation.testing.helper.Log;

public class YearEndValidChecksSQL {

    public static String YEAR_END_TABLE_PMC_COUNT =
            "SELECT count(*) as sourceCount FROM "+GetYearEndDbUser.getYearEndValidDB()+".year_end_full where pmc <> new_pmc";

    public static String PMC_CHANGES_FULL_COUNT =
            "SELECT count(*) as targetCount FROM "+GetYearEndDbUser.getYearEndValidDB()+".pmc_changes_full_v";

    public static String YEAR_END_TABLE_OPCO_RC_COUNT =
            "SELECT count(*) as sourceCount FROM "+GetYearEndDbUser.getYearEndValidDB()+".year_end_full where (opco_r12 <> new_opco_r12) or (rc <> new_rc_r12)";

    public static String OPCO_RC_CHANGES_FULL_COUNT =
            "SELECT count(*) as targetCount FROM "+GetYearEndDbUser.getYearEndValidDB()+".opco_rc_changes_full_v where new_rc_r12 is not null or new_opco_r12 is not null";

}
