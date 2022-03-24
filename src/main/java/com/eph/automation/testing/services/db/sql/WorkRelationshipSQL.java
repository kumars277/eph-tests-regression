package com.eph.automation.testing.services.db.sql;
//created by Nishant @ 5 May 2020
public class WorkRelationshipSQL {
  public static String getWorkParent ="select "
          + " f_parent as F_PARENT,f_child as F_CHILD,\n"
          + " f_relationship_type as F_RELATIONSHIP_TYPE,\n"
          + " effective_start_date as EFFECTIVE_START_DATE,\n"
          + " effective_end_date as EFFECTIVE_END_DATE"
          + " from semarchy_eph_mdm.gd_work_relationship where f_child='%s'";

    public static String getWorkChild="select "
            + " f_parent as F_PARENT,f_child as F_CHILD,\n"
            + " f_relationship_type as F_RELATIONSHIP_TYPE,\n"
            + " effective_start_date as EFFECTIVE_START_DATE,\n"
            + " effective_end_date as EFFECTIVE_END_DATE"
            + " from semarchy_eph_mdm.gd_work_relationship where f_parent='%s'";

}
