package com.eph.automation.testing.models.dao.StitchingExtended;
import java.util.HashMap;

public class ProdExtAvailJson {

    private ExtendedAvailability extendedAvailability;
    public ExtendedAvailability getExtendedAvailability() {return extendedAvailability;}
    public void setExtendedAvailability(ExtendedAvailability extendedAvailability) {this.extendedAvailability = extendedAvailability;}

    public static class ExtendedAvailability{
        private HashMap<String ,Object> type;
        public HashMap<String, Object> getType() {return type;}
        public void setType(HashMap<String, Object> type) {this.type = type;}


        private String count;
        public String getCount() {
            return count;}
        public void setCount(String count) {this.count = count;}
    }


}

