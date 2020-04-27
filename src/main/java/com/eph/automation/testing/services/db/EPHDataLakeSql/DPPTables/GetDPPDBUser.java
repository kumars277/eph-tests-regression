package com.eph.automation.testing.services.db.EPHDataLakeSql.DPPTables;

public class GetDPPDBUser {

    public static String[] getDppDataBase(){
        String DppdbDL  = null;
        String DppdbEPH = null;
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                DppdbDL = "product_database_sit";
                DppdbEPH = "researchpackages";
            }
            else{
                DppdbDL = "product_database_uat";
                DppdbEPH = "researchpackages";
            }

        }else{
            DppdbDL = "product_database_uat";
            DppdbEPH = "researchpackages";
        }
        return new String[]{DppdbEPH,DppdbDL};
    }
}
