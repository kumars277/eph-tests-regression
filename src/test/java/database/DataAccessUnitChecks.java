package database;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.configuration.LoadProperties;
import com.eph.automation.testing.models.contexts.DataQualityContext;
import com.eph.automation.testing.models.dao.ProductDataObject;
import com.eph.automation.testing.services.db.sql.ProductExtractSQL;
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
        System.out.println(DecryptionService.decrypt(LoadProperties.getProperty(Constants.EPH_SIT_URL)));
    }

    @Test
    @Ignore
    public void testEncrypt() {
        String postgres = "jdbc:postgresql://eph-sit-eph-sit-rds.cnnrbgocj0xg.eu-west-1.rds.amazonaws.com/ephsit?user=rdsappadmin&password=e)NWt4)0dq&ssl=false";
        System.out.println(DecryptionService.encrypt(postgres));
    }

    @Test
    @Ignore
    public void testPMXDBAccess() {
        String SQL = ProductExtractSQL.PMX_WORK_EXTRACT;
        dataQualityContext.productDataObjectsFromSource = DBManager.getDBResultAsBeanList(SQL, ProductDataObject.class, Constants.PMX_UAT_URL);
        System.out.println(dataQualityContext.productDataObjectsFromSource.get(0).PRODUCT_ID);
    }

    @Test
    @Ignore
    public void testPostgressDBAccess() {
        String SQL = ProductExtractSQL.PRODUCT_WORK_FROM_EPH_SA;
        dataQualityContext.productDataObjectsFromSource =
                DBManager.getDBResultAsBeanList(SQL, ProductDataObject.class, Constants.EPH_SIT_URL);
        System.out.println(dataQualityContext.productDataObjectsFromSource.size());
    }
}
