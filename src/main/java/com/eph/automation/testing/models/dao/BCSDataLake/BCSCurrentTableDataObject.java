//created by Nishant @ 21 Oct 2020
package com.eph.automation.testing.models.dao.BCSDataLake;


public class BCSCurrentTableDataObject {

    private String metadeleted;
    private String metamodifiedon;
    private String sourceref;

    private String classificationcode;
    private String value;
    private String classificationtype;
    private String priority;
    private String businessunit;

    private String originimpid;
    private String subgroup;
    private String series;
    private String copyrightyear;
    private String title;
    private String worktitle;
    private String isset;
    private String impressionid;
    private String doistatus;
    private String imprint;
    private String division;
    private String language3;
    private String regstatus;
    private String approvedondate;
    private String language2;
    private String doi;
    private String volumeno;
    private String editionid;
    private String synctemplate;
    private String volumename;
    private String seriesissn;
    private String seriesid;
    private String ownership;
    private String firstapproval;
    private String companygroup;
    private String shorttitle;
    private String editionno;
    private String work_master_flag;
    private String language;
    private String piidack;
    private String publisher;
    private String titleid;
    private String subtitle;
    private String seriescode;
    private String objecttype;

    //for current extObject
    private String object;
    private String type;
    private String name;
    private String comments;
    private String source;

    //for current fullversionfamily
    private String versiontype;
    private String isbn;
    private String projectno;
    private String workmaster;

    //for originatorAddress
    private String additionaladdress;
    private String addressid;
    private String addressline1;
    private String addressline2;
    private String addressline3;
    private String businesspartnerid;
    private String city;
    private String country;
    private String district;
    private String email;
    private String fax;
    private String houseno;
    private String internet;
    private String ismainaddress;
    private String mobile;
    private String postalcode;
    private String street;
    private String telephonemain;
    private String telephoneother;

    //for originators
    //private String metadeleted;
    //private String metamodifiedon;
    //private String sourceref;
    private String prefix;
    private String sequence;
    //private String businesspartnerid;
    private String originatorid;
    private String isperson;
    private String locationid;
    private String copyrightholdertype;
    private String institution;
    private String firstname;
    private String department;
    private String lastname;
    private String searchterm;

    //for stg_current_pricing
    //  private String metadeleted;
    //  private String metamodifiedon;
    //  private String sourceref;
    private String validfrom;
    //  private String type;
    private String currency;
    private String priceapprox;
    private String price;
    private String validto;


    public String getClassificationcode() {return classificationcode;}
    public void setClassificationcode(String classificationcode) {this.classificationcode = classificationcode;}

    public String getClassificationtype() {return classificationtype;}
    public void setClassificationtype(String classificationtype) {this.classificationtype = classificationtype;}

    public String getMetadeleted() {return metadeleted;}
    public void setMetadeleted(String metadeleted) {this.metadeleted = metadeleted;}

    public String getMetamodifiedon() {return metamodifiedon;}
    public void setMetamodifiedon(String metamodifiedon) {this.metamodifiedon = metamodifiedon;}

    public String getBusinessunit() {return businessunit;}
    public void setBusinessunit(String businessunit) {this.businessunit = businessunit;}

    public String getPriority() {return priority;}
    public void setPriority(String priority) {this.priority = priority;}

    public String getSourceref() {return sourceref;}
    public void setSourceref(String sourceref) {this.sourceref = sourceref;}

    public String getValue() {return value;}
    public void setValue(String value) {this.value = value;}

    public String getOriginimpid() {
        return originimpid;
    }

    public void setOriginimpid(String originimpid) {
        this.originimpid = originimpid;
    }

    public String getSubgroup() {
        return subgroup;
    }

    public void setSubgroup(String subgroup) {
        this.subgroup = subgroup;
    }

    public String getSeries() {
        return series;
    }

    public void setSeries(String series) {
        this.series = series;
    }

    public String getSeriesissn() {
        return seriesissn;
    }

    public void setSeriesissn(String seriesissn) {
        this.seriesissn = seriesissn;
    }

    public String getSeriescode() {
        return seriescode;
    }

    public void setSeriescode(String seriescode) {
        this.seriescode = seriescode;
    }

    public String getSeriesid() {
        return seriesid;
    }

    public void setSeriesid(String seriesid) {
        this.seriesid = seriesid;
    }

    public String getCopyrightyear() {
        return copyrightyear;
    }

    public void setCopyrightyear(String copyrightyear) {
        this.copyrightyear = copyrightyear;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getTitleid() {
        return titleid;
    }

    public void setTitleid(String titleid) {
        this.titleid = titleid;
    }

    public String getSubtitle() {
        return subtitle;
    }

    public void setSubtitle(String subtitle) {
        this.subtitle = subtitle;
    }


    public String getWorktitle() {
        return worktitle;
    }

    public void setWorktitle(String worktitle) {
        this.worktitle = worktitle;
    }

    public String getWork_master_flag() {
        return work_master_flag;
    }

    public void setWork_master_flag(String work_master_flag) {
        this.work_master_flag = work_master_flag;
    }

    public String getIsset() {
        return isset;
    }

    public void setIsset(String isset) {
        this.isset = isset;
    }

    public String getImpressionid() {
        return impressionid;
    }

    public void setImpressionid(String impressionid) {
        this.impressionid = impressionid;
    }

    public String getDoistatus() {
        return doistatus;
    }

    public void setDoistatus(String doistatus) {
        this.doistatus = doistatus;
    }

    public String getImprint() {
        return imprint;
    }

    public void setImprint(String imprint) {
        this.imprint = imprint;
    }

    public String getDivision() {
        return division;
    }

    public void setDivision(String division) {
        this.division = division;
    }

    public String getLanguage() {
        return language;
    }

    public void setLanguage(String language) {
        this.language = language;
    }

    public String getLanguage2() {
        return language2;
    }

    public void setLanguage2(String language2) {
        this.language2 = language2;
    }

    public String getLanguage3() {
        return language3;
    }

    public void setLanguage3(String language3) {
        this.language3 = language3;
    }

    public String getRegstatus() {
        return regstatus;
    }

    public void setRegstatus(String regstatus) {
        this.regstatus = regstatus;
    }

    public String getApprovedondate() {
        return approvedondate;
    }

    public void setApprovedondate(String approvedondate) {
        this.approvedondate = approvedondate;
    }

    public String getDoi() {
        return doi;
    }

    public void setDoi(String doi) {
        this.doi = doi;
    }

    public String getVolumeno() {
        return volumeno;
    }

    public void setVolumeno(String volumeno) {
        this.volumeno = volumeno;
    }

    public String getVolumename() {
        return volumename;
    }

    public void setVolumename(String volumename) {
        this.volumename = volumename;
    }

    public String getEditionid() {
        return editionid;
    }

    public void setEditionid(String editionid) {
        this.editionid = editionid;
    }

    public String getEditionno() {
        return editionno;
    }

    public void setEditionno(String editionno) {
        this.editionno = editionno;
    }

    public String getSynctemplate() {
        return synctemplate;
    }

    public void setSynctemplate(String synctemplate) {
        this.synctemplate = synctemplate;
    }

    public String getOwnership() {
        return ownership;
    }

    public void setOwnership(String ownership) {
        this.ownership = ownership;
    }

    public String getFirstapproval() {
        return firstapproval;
    }

    public void setFirstapproval(String firstapproval) {
        this.firstapproval = firstapproval;
    }

    public String getCompanygroup() {
        return companygroup;
    }

    public void setCompanygroup(String companygroup) {
        this.companygroup = companygroup;
    }

    public String getShorttitle() {
        return shorttitle;
    }

    public void setShorttitle(String shorttitle) {
        this.shorttitle = shorttitle;
    }

    public String getPiidack() {
        return piidack;
    }

    public void setPiidack(String piidack) {
        this.piidack = piidack;
    }

    public String getPublisher() {
        return publisher;
    }

    public void setPublisher(String publisher) {
        this.publisher = publisher;
    }

    public String getObjecttype() {
        return objecttype;
    }

    public void setObjecttype(String objecttype) {
        this.objecttype = objecttype;
    }

    public String getComments() {return comments;}
    public void setComments(String comments) {this.comments = comments;}

    public String getName() {return name;}
    public void setName(String name) {this.name = name;}

    public String getObject() {return object;}
    public void setObject(String object) {this.object = object;}

    public String getType() {return type;}
    public void setType(String type) {this.type = type;}

    public String getSource() {return source;}
    public void setSource(String source) {this.source = source;}


    public String getVersiontype() {return versiontype;}
    public void setVersiontype(String versiontype) {this.versiontype = versiontype;}

    public String getIsbn() {return isbn;}
    public void setIsbn(String isbn) {this.isbn = isbn;}

    public String getProjectno() {return projectno;}
    public void setProjectno(String projectno) {this.projectno = projectno;}

    public String getWorkmaster() {return workmaster;}
    public void setWorkmaster(String workmaster) {this.workmaster = workmaster;}

    public String getAdditionaladdress() {return additionaladdress;}
    public void setAdditionaladdress(String additionaladdress) {this.additionaladdress = additionaladdress;}

    public String getAddressid() {return addressid;}
    public void setAddressid(String addressid) {this.addressid = addressid;}

    public String getAddressline1() {return addressline1;}
    public void setAddressline1(String addressline1) {this.addressline1 = addressline1;}

    public String getAddressline2() {return addressline2;}
    public void setAddressline2(String addressline2) {this.addressline2 = addressline2;}

    public String getAddressline3() {return addressline3;}
    public void setAddressline3(String addressline3) {this.addressline3 = addressline3;}

    public String getBusinesspartnerid() {return businesspartnerid;}
    public void setBusinesspartnerid(String businesspartnerid) {this.businesspartnerid = businesspartnerid;}

    public String getCity() {return city;}
    public void setCity(String city) {this.city = city;}

    public String getCountry() {return country;}
    public void setCountry(String country) {this.country = country;}

    public String getDistrict() {return district;}
    public void setDistrict(String district) {this.district = district;}

    public String getEmail() {return email;}
    public void setEmail(String email) {this.email = email;}

    public String getFax() {return fax;}
    public void setFax(String fax) {this.fax = fax;}

    public String getHouseno() {return houseno;}
    public void setHouseno(String houseno) {this.houseno = houseno;}

    public String getInternet() {return internet;}
    public void setInternet(String internet) {this.internet = internet;}

    public String getIsmainaddress() {return ismainaddress;}
    public void setIsmainaddress(String ismainaddress) {this.ismainaddress = ismainaddress;}

    public String getMobile() {return mobile;}
    public void setMobile(String mobile) {this.mobile = mobile;}

    public String getPostalcode() {return postalcode;}
    public void setPostalcode(String postalcode) {this.postalcode = postalcode;    }

    public String getStreet() {return street;}
    public void setStreet(String street) {this.street = street;}

    public String getTelephonemain() {return telephonemain;}
    public void setTelephonemain(String telephonemain) {this.telephonemain = telephonemain;}

    public String getTelephoneother() {return telephoneother;}
    public void setTelephoneother(String telephoneother) {this.telephoneother = telephoneother;}

    public String getPrefix() {return prefix;}
    public void setPrefix(String prefix) {this.prefix = prefix;}

    public String getSequence() {return sequence;}
    public void setSequence(String sequence) {this.sequence = sequence;}

    public String getOriginatorid() {return originatorid;}
    public void setOriginatorid(String originatorid) {this.originatorid = originatorid;}

    public String getIsperson() {return isperson;}
    public void setIsperson(String isperson) {this.isperson = isperson;}

    public String getLocationid() {return locationid;}
    public void setLocationid(String locationid) {this.locationid = locationid;}

    public String getCopyrightholdertype() {return copyrightholdertype;}
    public void setCopyrightholdertype(String copyrightholdertype) {this.copyrightholdertype = copyrightholdertype;}

    public String getInstitution() {return institution;}
    public void setInstitution(String institution) {this.institution = institution;}

    public String getFirstname() {return firstname;}
    public void setFirstname(String firstname) {this.firstname = firstname;}

    public String getDepartment() {return department;}
    public void setDepartment(String department) {this.department = department;}

    public String getLastname() {return lastname;}
    public void setLastname(String lastname) {this.lastname = lastname;}

    public String getSearchterm() {return searchterm;}
    public void setSearchterm(String searchterm) {this.searchterm = searchterm;}

    public String getCurrency() {return currency;}
    public void setCurrency(String currency) {this.currency = currency;}

    public String getPrice() {return price;}
    public void setPrice(String price) {this.price = price;}

    public String getPriceapprox() {return priceapprox;}
    public void setPriceapprox(String priceapprox) {this.priceapprox = priceapprox;}

    public String getValidfrom() {return validfrom;}
    public void setValidfrom(String validfrom) {this.validfrom = validfrom;}

    public String getValidto() {return validto;}
    public void setValidto(String validto) {this.validto = validto;}
}
