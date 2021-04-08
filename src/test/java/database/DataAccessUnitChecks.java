package database;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.configuration.LoadProperties;
import com.eph.automation.testing.models.contexts.DataQualityContext;
import com.eph.automation.testing.models.dao.WorkDataObject;
import com.eph.automation.testing.services.db.sql.GetEPHDBUser;
import com.eph.automation.testing.services.db.sql.WorkExtractSQL;
import com.eph.automation.testing.services.security.DecryptionService;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Created by RAVIVARMANS on 24/11/2018.
 */
public class DataAccessUnitChecks {

    @StaticInjection
    public static DataQualityContext dataQualityContext;

    @Test
    @Ignore
    public void testDecrypt() {
        System.out.println(DecryptionService.decrypt("D850IuF7l4WxllwIa6PhOhqZ+fRSCXa90GeIPTMzCapIh/U5i/M5HsxI5rK1CgeyRPGHN6zMGj9yyd+VPmvEpEoFmocNl4UGFFdmrCcq0l1z4JGWMK6CiCGVlUXqrIC1IeVB3IeYqjaJvMnNjOnDfrYC6eQxtXciGO/dvizn2DrSOW78nqjaVzq/cDEb3YLHN8zrrdTRiBU="));
    }

    @Test
    @Ignore
    public void testEncrypt() {
           System.out.println(DecryptionService.encrypt("jdbc:postgresql://eph-uat-eph-uat2-rds.cucvf0thmu0s.eu-west-1.rds.amazonaws.com/ephuat2?user=semarchy_eph_mdm_ro&password=*U38Gbb-sy&ssl=false"));


    }

    @Test
    @Ignore
    public void testPMXDBAccess() {
        String SQL = WorkExtractSQL.PMX_WORK_EXTRACT.replace("PARAM1","9781416049722");
        dataQualityContext.workDataObjectsFromSource = DBManager.getDBResultAsBeanList(SQL, WorkDataObject.class, Constants.PMX_URL);
        System.out.println(dataQualityContext.workDataObjectsFromSource.get(0).getPRIMARY_ISBN());
    }

    @Test
    @Ignore
    public void testPostgressDBAccess() {
        String SQL = WorkExtractSQL.PRODUCT_MANIFESTATION_FROM_EPH_SA.replace("PARAM1","9781416049722");
        dataQualityContext.workDataObjectsFromSource =
                DBManager.getDBResultAsBeanList(SQL, WorkDataObject.class, Constants.EPH_URL);
        System.out.println(dataQualityContext.workDataObjectsFromSource.get(0).getPRIMARY_ISBN()
        );
    }
}
