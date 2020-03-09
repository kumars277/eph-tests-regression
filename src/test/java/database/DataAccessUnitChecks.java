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
        //PMX_UAT_URL=jdbc:oracle:thin:PMX/pmxuat@//pmxuat.cucvf0thmu0s.eu-west-1.rds.amazonaws.com:1521/PMXUAT
        System.out.println(DecryptionService.decrypt(LoadProperties.getProperty(Constants.EPH_URL)));
    }

    @Test
    @Ignore
    public void testEncrypt() {
          // System.out.println(DecryptionService.encrypt(athena));
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
