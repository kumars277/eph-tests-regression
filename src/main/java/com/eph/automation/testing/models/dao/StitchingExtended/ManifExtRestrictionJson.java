package com.eph.automation.testing.models.dao.StitchingExtended;
import java.util.HashMap;

public class ManifExtRestrictionJson {

    private ExtendedRestriction extendedRestriction;
    public ExtendedRestriction getExtendedRestriction() {return extendedRestriction;}
    public void setExtendedRestriction(ExtendedRestriction extendedRestriction) {this.extendedRestriction = extendedRestriction;}

    public static class ExtendedRestriction{
        private HashMap<String ,Object> type;
        public HashMap<String, Object> getType() {return type;}
        public void setType(HashMap<String, Object> type) {this.type = type;}

    }


}

