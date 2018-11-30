package com.eph.automation.testing.services.talend;

import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.models.batch.JobConfiguration;
import com.eph.automation.testing.services.db.sql.TalendJobSQL;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by RAVIVARMANS on 30/11/2018.
 */
public class TalendUtils {

    public static Map<String, String> getContextParamsForRinggoldIncremental() {
        Map<String, String> contextMap = new HashMap<>();
        contextMap.put("Load_Type", "Incremental");
        contextMap.put("RG_Run_FTP", "NO");
        contextMap.put("RG_Run_Incr_Consortia", "NO");
        contextMap.put("RG_Run_Wrapper_Consortia", "NO");
        contextMap.put("Run_Consortia_Batch", "NO");
        contextMap.put("Run_Batch", "YES");

        return contextMap;
    }

    public static Map<String, String> reSetRUNBatch() {
        Map<String, String> contextMap = new HashMap<>();
        contextMap.put("Run_Batch", "YES");
        return contextMap;
    }

    public static Map<String, String> runWithOrWithoutBatch(String batchState) {
        Map<String, String> contextMap = new HashMap<>();
        contextMap.put("Run_Batch", batchState);
        return contextMap;
    }

    public static void performUpdate(String projectName, Map contextMap, String dbEndPoint) {
        Iterator it = contextMap.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry contextKeyAndValue = (Map.Entry) it.next();
            int queryOutPut = DBManager.mysqlConnection(TalendJobSQL.UPDATE_TALEND_CONTEXT_VALUE
                    .replace("PARAM1", String.valueOf(contextKeyAndValue.getValue()))
                    .replace("PARAM2", projectName)
                    .replace("PARAM3", String.valueOf(contextKeyAndValue.getKey())), JobConfiguration.class, dbEndPoint);
            it.remove(); // avoids a ConcurrentModificationException
        }
    }
}
