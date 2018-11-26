package database;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.configuration.LoadProperties;
import com.eph.automation.testing.models.contexts.DataQualityContext;
import com.eph.automation.testing.models.dao.ProductDataObject;
import com.eph.automation.testing.services.db.sql.ProductExtractSQL;
import com.eph.automation.testing.services.decryption.DecryptionService;
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
        //            jdbc:oracle:thin:CMX_TALEND/3hI86iCB$6J6qX@//cmxuat.cucvf0thmu0s.eu-west-1.rds.amazonaws.com:1521/CMXUAT
        System.out.println(DecryptionService.decrypt(LoadProperties.getProperty(Constants.CMX_UAT_DB_URL_KEY)));
    }

    @Test
    @Ignore
    public void testEncrypt() {
        System.out.println(DecryptionService.encrypt(LoadProperties.getProperty(Constants.PMX_UAT_URL)));
    }

    @Test
    @Ignore
    public void testDBStaging() {
        String SQL = ProductExtractSQL.PMX_WORK_EXTRACT;
        dataQualityContext.productDataObjectsFromSource = DBManager.getDBResultAsEntity(ProductDataObject.class, Constants.PMX_UAT_URL,SQL);
        System.out.println(dataQualityContext.productDataObjectsFromSource.get(0).PRODUCT_ID);
    }
}
