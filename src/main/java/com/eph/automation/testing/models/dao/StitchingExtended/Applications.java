package com.eph.automation.testing.models.dao.StitchingExtended;

public class Applications {

    private String applicationName;
    private String deltaAnswerCodeUK;
    private String deltaAnswerCodeUS;
    private String publicationStatusANZ;
    private String availabilityStartDate;
    private String availabilityStatus;
    private String availabilityFormat;

    private String currency;
    private String  amount;
    private String startDate;
    private String category;
    private String endDate;
    private String region;
    private String customerCategory;
    private String purchaseQuantity;


    public String getCurrency() { return currency;  }
    public void setCurrency(String currency) {
        this.currency = currency;
    }

    public String getPurchaseQuantity() { return purchaseQuantity;  }
    public void setPurchaseQuantity(String purchaseQuantity) {
        this.purchaseQuantity = purchaseQuantity;
    }

    public String getCustomerCategory() { return customerCategory;  }
    public void setCustomerCategory(String customerCategory) {
        this.customerCategory = customerCategory;
    }

    public String getCategory() { return category;  }
    public void setCategory(String category) {
        this.category = category;
    }

    public String getEndDate() { return endDate;  }
    public void setEndDate(String endDate) {
        this.endDate = endDate;
    }

    public String getRegion() { return region;  }
    public void setRegion(String region) {
        this.region = region;
    }

    public String getStartDate() { return startDate;  }
    public void setStartDate(String startDate) {
        this.startDate = startDate;
    }

    public String getAmount() { return amount;  }
    public void setAmount(String amount) {
        this.amount = amount;
    }


    public String getApplicationName() { return applicationName;  }
    public void setApplicationName(String applicationName) {
        this.applicationName = applicationName;
    }

    public String getAvailabilityFormat() { return availabilityFormat;  }
    public void setAvailabilityFormat(String availabilityFormat) {
        this.availabilityFormat = availabilityFormat;
    }

    public String getDeltaAnswerCodeUK() {return deltaAnswerCodeUK;}
    public void setDeltaAnswerCodeUK(String deltaAnswerCodeUK) {
        this.deltaAnswerCodeUK = deltaAnswerCodeUK;
    }

    public String getDeltaAnswerCodeUS() {return deltaAnswerCodeUS; }
    public void setDeltaAnswerCodeUS(String deltaAnswerCodeUS) {
        this.deltaAnswerCodeUS = deltaAnswerCodeUS;
    }

    public String getPublicationStatusANZ() {return publicationStatusANZ; }
    public void setPublicationStatusANZ(String publicationStatusANZ) {
        this.publicationStatusANZ = publicationStatusANZ;
    }

    public String getAvailabilityStartDate() {return availabilityStartDate; }
    public void setAvailabilityStartDate(String availabilityStartDate) {
        this.availabilityStartDate = availabilityStartDate;
    }

    public String getAvailabilityStatus() {return availabilityStatus; }
    public void setAailabilityStatus(String availabilityStatus) {
        this.availabilityStatus = availabilityStatus;
    }

    /*
    private String applicationName;
    public String getApplicationName() {return applicationName;}
    public void setApplicationName(String applicationName) {this.applicationName = applicationName;}
*/
   /* public static class ExtendedPageCount{
        private HashMap<String ,Object> type;
        public HashMap<String, Object> getType() {return type;}
        public void setType(HashMap<String, Object> type) {this.type = type;}


        private String count;
        public String getCount() {
            return count;}
        public void setCount(String count) {this.count = count;}
    }*/


}

