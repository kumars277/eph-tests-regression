package com.eph.automation.testing.models.dao.StitchingExtended;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.DataQualityContext;
import com.eph.automation.testing.models.contexts.JRBIAccessDLContext;
import org.junit.Assert;
import java.util.HashMap;
import com.eph.automation.testing.models.dao.StitchingExtended.ManifExtJson;

public class ManifExtPgCountJson {

    private ExtendedPageCount extendedPageCount;
    public ExtendedPageCount getExtendedPageCount() {return extendedPageCount;}
    public void setExtendedPageCount(ExtendedPageCount extendedPageCount) {this.extendedPageCount = extendedPageCount;}

    public static class ExtendedPageCount{
        private HashMap<String ,Object> type;
        public HashMap<String, Object> getType() {return type;}
        public void setType(HashMap<String, Object> type) {this.type = type;}


        private String count;
        public String getCount() {
            return count;}
        public void setCount(String count) {this.count = count;}
    }


}

