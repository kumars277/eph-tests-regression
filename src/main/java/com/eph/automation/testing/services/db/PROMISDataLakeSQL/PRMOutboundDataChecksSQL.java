package com.eph.automation.testing.services.db.PROMISDataLakeSQL;

public class PRMOutboundDataChecksSQL {

    public static String GET_PUBIDS_IDS = "select PUB_IDT from (SELECT PUB_IDT FROM PRM.PRMAUTPUBT ORDER BY dbms_random.value) WHERE ROWNUM<=%s";

    public static String GET_CLSCOD_IDS = "select CLS_COD from (SELECT CLS_COD FROM PRM.PRMCLSCODT ORDER BY dbms_random.value) WHERE ROWNUM<=%s";

    public static String GET_CLSTPUBIDT_IDS = "select PUB_IDT from (SELECT PUB_IDT FROM PRM.PRMCLST ORDER BY dbms_random.value) WHERE ROWNUM<=%s";

    public static String GET_PUBIDT_LONDEST_IDS = "select PUB_IDT from (SELECT PUB_IDT FROM PRM.PRMLONDEST ORDER BY dbms_random.value) WHERE ROWNUM<=%s";

    public static String GET_PUBIDT_PRICEST_IDS = "select PUB_IDT from (SELECT PUB_IDT FROM PRM.PRMPRICEST ORDER BY dbms_random.value) WHERE ROWNUM<=%s";

    public static String GET_PUBIDT_PUBINFT_IDS = "select PUB_IDT from (SELECT PUB_IDT FROM PRM.PRMPUBINFT ORDER BY dbms_random.value) WHERE ROWNUM<=%s";

    public static String GET_PUBIDT_PUBRELT_IDS = "select PUB_PUB_IDT from (SELECT PUB_PUB_IDT FROM PRM.PRMPUBRELT ORDER BY dbms_random.value) WHERE ROWNUM<=%s";

    public static String getAutPubtSql(String serverEnv, String table) {
        String GET_DATA_AUTPUBT_PRM = null;
        switch (serverEnv) {
            case ("oracle"):
                GET_DATA_AUTPUBT_PRM = "select \n" +
                        "PUB_IDT as PUB_IDT\n" +
                        ",AUT_EDT_IDT as AUT_EDT_IDT\n" +
                        ",AUT_EDT_TYP as AUT_EDT_TYP\n" +
                        ",TYP_DES as TYP_DES\n" +
                        ",SEQ_NUM as SEQ_NUM\n" +
                        ",AUT_EDT_PRE as AUT_EDT_PRE\n" +
                        ",AUT_EDT_INI as AUT_EDT_INI\n" +
                        ",AUT_EDT_NAM as AUT_EDT_NAM\n" +
                        ",AUT_EDT_SORT as AUT_EDT_SORT\n" +
                        ",AUT_EDT_SUF as AUT_EDT_SUF\n" +
                        ",AFF_TXT as AFF_TXT\n" +
                        ",FTN as FTN\n" +
                       // ",BIO as BIO\n" + //problem with BIO Column throws nullpointer removed for now it holds Long datatype
                        ",AUT_EDT_FAX as AUT_EDT_FAX\n" +
                        ",AUT_EDT_PHONE as AUT_EDT_PHONE\n" +
                        ",AUT_EDT_EMAIL as AUT_EDT_EMAIL\n" +
                        ",AUT_EDT_JCI as AUT_EDT_JCI\n" +
                        ",BIO_IMAGE as BIO_IMAGE\n" +
                        "from PRM" + "." + table + "\n" +
                        " where PUB_IDT in ('%s')";
                break;
            case ("aws"):
                GET_DATA_AUTPUBT_PRM = "select \n" +
                        "PUB_IDT as PUB_IDT\n" +
                        ",AUT_EDT_IDT as AUT_EDT_IDT\n" +
                        ",AUT_EDT_TYP as AUT_EDT_TYP\n" +
                        ",TYP_DES as TYP_DES\n" +
                        ",SEQ_NUM as SEQ_NUM\n" +
                        ",AUT_EDT_PRE as AUT_EDT_PRE\n" +
                        ",AUT_EDT_INI as AUT_EDT_INI\n" +
                        ",AUT_EDT_NAM as AUT_EDT_NAM\n" +
                        ",AUT_EDT_SORT as AUT_EDT_SORT\n" +
                        ",AUT_EDT_SUF as AUT_EDT_SUF\n" +
                        ",AFF_TXT as AFF_TXT\n" +
                        ",FTN as FTN\n" +
                    //    ",BIO as BIO\n" +
                        ",AUT_EDT_FAX as AUT_EDT_FAX\n" +
                        ",AUT_EDT_PHONE as AUT_EDT_PHONE\n" +
                        ",AUT_EDT_EMAIL as AUT_EDT_EMAIL\n" +
                        ",AUT_EDT_JCI as AUT_EDT_JCI\n" +
                        ",BIO_IMAGE as BIO_IMAGE\n" +
                        "from " + GetPRMDLDBUser.getPRMDataBase() + ".PROMIS_" + table + "_PART\n" +
                        " where inbound_ts = (select inbound_ts from " + GetPRMDLDBUser.getPRMDataBase() + ".PROMIS_" + table + "_PART order by inbound_ts desc limit 1)\n" +
                        " AND PUB_IDT in ('%s')";
                break;
        }
        return GET_DATA_AUTPUBT_PRM;
    }

    public static String getClsCodSql(String serverEnv, String table) {
        String GET_DATA_CLSCOD_PRM = null;
        switch (serverEnv) {
            case ("oracle"):
                GET_DATA_CLSCOD_PRM = "select \n" +
                        "CLS_COD as CLS_COD\n" +
                        ",CLS_DES as CLS_DES\n" +
                        ",CLS_GRP_COD as CLS_GRP_COD\n" +
                        "from PRM" + "." + table + "\n" +
                        " where CLS_COD in ('%s')";
                break;
            case ("aws"):
                GET_DATA_CLSCOD_PRM = "select \n" +
                        "CLS_COD as CLS_COD\n" +
                        ",CLS_DES as CLS_DES\n" +
                        ",CLS_GRP_COD as CLS_GRP_COD\n" +
                        "from " + GetPRMDLDBUser.getPRMDataBase() + ".PROMIS_" + table + "_PART\n" +
                        " where inbound_ts = (select inbound_ts from " + GetPRMDLDBUser.getPRMDataBase() + ".PROMIS_" + table + "_PART order by inbound_ts desc limit 1)\n" +
                        " AND CLS_COD in ('%s')";
                break;
        }
        return GET_DATA_CLSCOD_PRM;

    }

    public static String getClstSql(String serverEnv, String table) {
        String GET_DATA_CLST_PRM = null;
        switch (serverEnv) {
            case ("oracle"):
                GET_DATA_CLST_PRM = "select \n" +
                        "PUB_IDT as PUB_IDT\n" +
                        ",CLS_COD as CLS_COD\n" +
                        ",SCA_COD as SCA_COD\n" +
                        "from PRM" + "." + table + "\n" +
                        " where PUB_IDT in ('%s')";
                break;
            case ("aws"):
                GET_DATA_CLST_PRM = "select \n" +
                        "PUB_IDT as PUB_IDT\n" +
                        ",CLS_COD as CLS_COD\n" +
                        ",SCA_COD as SCA_COD\n" +
                        "from " + GetPRMDLDBUser.getPRMDataBase() + ".PROMIS_" + table + "_PART\n" +
                        " where inbound_ts = (select inbound_ts from " + GetPRMDLDBUser.getPRMDataBase() + ".PROMIS_" + table + "_PART order by inbound_ts desc limit 1)\n" +
                        " AND PUB_IDT in ('%s')";
                break;
        }
        return GET_DATA_CLST_PRM;

    }

    public static String getLonDestSql(String serverEnv, String table) {
        String GET_DATA_LONDEST_PRM = null;
        switch (serverEnv) {
            case ("oracle"):
                GET_DATA_LONDEST_PRM = "select \n" +
                        "PUB_IDT as PUB_IDT\n" +
                        ",PUB_VOL_IDT as PUB_VOL_IDT\n" +
                        ",VOL_PRT_IDT as VOL_PRT_IDT\n" +
                       // ",LON_DES as LON_DES\n" + Data Type Long Not supported in the framework
                        "from PRM" + "." + table + "\n" +
                        " where PUB_IDT in ('%s')";
                break;
            case ("aws"):
                GET_DATA_LONDEST_PRM = "select \n" +
                        "PUB_IDT as PUB_IDT\n" +
                        ",PUB_VOL_IDT as PUB_VOL_IDT\n" +
                        ",VOL_PRT_IDT as VOL_PRT_IDT\n" +
                       // ",LON_DES as LON_DES\n" +
                        "from " + GetPRMDLDBUser.getPRMDataBase() + ".PROMIS_" + table + "_PART\n" +
                        " where inbound_ts = (select inbound_ts from " + GetPRMDLDBUser.getPRMDataBase() + ".PROMIS_" + table + "_PART order by inbound_ts desc limit 1)\n" +
                        " AND PUB_IDT in ('%s')";
                break;
        }
        return GET_DATA_LONDEST_PRM;

    }

    public static String getPriCestSql(String serverEnv, String table) {
        String GET_DATA_PRICEST_PRM = null;
        switch (serverEnv) {
            case ("oracle"):
                GET_DATA_PRICEST_PRM = "select \n" +
                        "PUB_IDT as PUB_IDT\n" +
                        ",PUB_VOL_IDT as PUB_VOL_IDT\n" +
                        ",VOL_PRT_IDT as VOL_PRT_IDT\n" +
                        ",EDN_IDT as EDN_IDT\n" +
                        ",PRC_TYP as PRC_TYP\n" +
                        ",PRC_GEO as PRC_GEO\n" +
                        ",IPT_COD as IPT_COD\n" +
                        ",PRC_DAT as PRC_DAT\n" +
                        ",STD_CUR_COD as STD_CUR_COD\n" +
                        ",STD_PRC as STD_PRC\n" +
                        ",PRC_PRE as PRC_PRE\n" +
                        ",ADD_PRC as ADD_PRC\n" +
                        ",FLAG as FLAG\n" +
                        ",EXP_DAT as EXP_DAT\n" +
                        "from PRM" + "." + table + "\n" +
                        " where PUB_IDT in ('%s')";
                break;
            case ("aws"):
                GET_DATA_PRICEST_PRM = "select \n" +
                        "PUB_IDT as PUB_IDT\n" +
                        ",PUB_VOL_IDT as PUB_VOL_IDT\n" +
                        ",VOL_PRT_IDT as VOL_PRT_IDT\n" +
                        ",EDN_IDT as EDN_IDT\n" +
                        ",PRC_TYP as PRC_TYP\n" +
                        ",PRC_GEO as PRC_GEO\n" +
                        ",IPT_COD as IPT_COD\n" +
                        ",PRC_DAT as PRC_DAT\n" +
                        ",STD_CUR_COD as STD_CUR_COD\n" +
                        ",STD_PRC as STD_PRC\n" +
                        ",PRC_PRE as PRC_PRE\n" +
                        ",ADD_PRC as ADD_PRC\n" +
                        ",FLAG as FLAG\n" +
                        ",EXP_DAT as EXP_DAT\n" +
                        "from " + GetPRMDLDBUser.getPRMDataBase() + ".PROMIS_" + table + "_PART\n" +
                        " where inbound_ts = (select inbound_ts from " + GetPRMDLDBUser.getPRMDataBase() + ".PROMIS_" + table + "_PART order by inbound_ts desc limit 1)\n" +
                        " AND PUB_IDT in ('%s')";
                break;
        }
        return GET_DATA_PRICEST_PRM;
    }

    public static String getPubInftSql(String serverEnv, String table) {
        String GET_DATA_PUBINFT_PRM = null;
        switch (serverEnv) {
            case ("oracle"):
                GET_DATA_PUBINFT_PRM = "select \n" +
                        "PUB_IDT as PUB_IDT\n" +
                        ",STA_DAA as STA_DAA\n" +
                        ",FUL_TIT as FUL_TIT\n" +
                        ",PUB_TYP as PUB_TYP\n" +
                        ",OWN_IDT as OWN_IDT\n" +
                        ",PBL_ABR_NAM as PBL_ABR_NAM\n" +
                        ",LOC as LOC\n" +
                        ",STA_PRM as STA_PRM\n" +
                        ",LNG_COD as LNG_COD\n" +
                        ",PUB_IMP_IDT as PUB_IMP_IDT\n" +
                        ",PUB_BGN_YEA as PUB_BGN_YEA\n" +
                        ",LCO_NUM as LCO_NUM\n" +
                        ",IMP_DAT as IMP_DAT\n" +
                        ",CDA as CDA\n" +
                        ",CRE_IDT as CRE_IDT\n" +
                        ",DEP_IDT as DEP_IDT\n" +
                        ",LST_USR_IDT as LST_USR_IDT\n" +
                        ",LST_UPD_DAT as LST_UPD_DAT\n" +
                        ",ABR_TIT as ABR_TIT\n" +
                        ",FUL_TIT_SRT as FUL_TIT_SRT\n" +
                        ",SUB_TIT_1 as SUB_TIT_1\n" +
                        ",SUB_TIT_2 as SUB_TIT_2\n" +
                        ",SUB_TIT_3 as SUB_TIT_3\n" +
                        ",PRG_SUB_TIT as PRG_SUB_TIT\n" +
                        ",AUT_EDT_NAM as AUT_EDT_NAM\n" +
                        ",SLO_TXT as SLO_TXT\n" +
                        ",NTB as NTB\n" +
                        ",FTN as FTN\n" +
                        ",FOR_TIT as FOR_TIT\n" +
                        ",RPN_INF as RPN_INF\n" +
                        ",ADV_COD as ADV_COD\n" +
                        ",RVW_COD as RVW_COD\n" +
                        ",SUP_APP as SUP_APP\n" +
                        ",PHY_SZE as PHY_SZE\n" +
                        ",FS_IDT as FS_IDT\n" +
                        ",WTK_NUM as WTK_NUM\n" +
                        ",WTK_CLS as WTK_CLS\n" +
                        ",BWK as BWK\n" +
                        ",AEI as AEI\n" +
                        ",REV_EDN_TI as REV_EDN_TI\n" +
                        ",JNL_IDT as JNL_IDT\n" +
                        ",LST_UPD_DATE as LST_UPD_DATE\n" +
                        ",CDA_DATE as CDA_DATE\n" +
                        ",AQS_COD as AQS_COD\n" +
                        ",MKT_COD as MKT_COD\n" +
                        ",AUD as AUD\n" +
                        ",SHT_DES as SHT_DES\n" +
                        ",SHT_CTT_DES as SHT_CTT_DES\n" +
                        ",SLO_EXP_DAT as SLO_EXP_DAT\n" +
                        ",IF_NO as IF_NO\n" +
                        ",IF_YEAR as IF_YEAR\n" +
                        ",IF_COMMENT as IF_COMMENT\n" +
                        ",AUT_BENEFITS as AUT_BENEFITS\n" +
                        ",SOURCE as SOURCE\n" +
                        ",ARG_COD as ARG_COD\n" +
                        ",IF_5 as IF_5\n" +
                        ",IF_RANKING as IF_RANKING\n" +
                        ",IF_CAT as IF_CAT\n" +
                        ",OA_TYPE as OA_TYPE\n" +
                        "from PRM" + "." + table + "\n" +
                        " where PUB_IDT in ('%s')";
                break;
            case ("aws"):
                GET_DATA_PUBINFT_PRM =  "select \n" +
                        "PUB_IDT as PUB_IDT\n" +
                        ",STA_DAA as STA_DAA\n" +
                        ",FUL_TIT as FUL_TIT\n" +
                        ",PUB_TYP as PUB_TYP\n" +
                        ",OWN_IDT as OWN_IDT\n" +
                        ",PBL_ABR_NAM as PBL_ABR_NAM\n" +
                        ",LOC as LOC\n" +
                        ",STA_PRM as STA_PRM\n" +
                        ",LNG_COD as LNG_COD\n" +
                        ",PUB_IMP_IDT as PUB_IMP_IDT\n" +
                        ",PUB_BGN_YEA as PUB_BGN_YEA\n" +
                        ",LCO_NUM as LCO_NUM\n" +
                        ",IMP_DAT as IMP_DAT\n" +
                        ",CDA as CDA\n" +
                        ",CRE_IDT as CRE_IDT\n" +
                        ",DEP_IDT as DEP_IDT\n" +
                        ",LST_USR_IDT as LST_USR_IDT\n" +
                        ",LST_UPD_DAT as LST_UPD_DAT\n" +
                        ",ABR_TIT as ABR_TIT\n" +
                        ",FUL_TIT_SRT as FUL_TIT_SRT\n" +
                        ",SUB_TIT_1 as SUB_TIT_1\n" +
                        ",SUB_TIT_2 as SUB_TIT_2\n" +
                        ",SUB_TIT_3 as SUB_TIT_3\n" +
                        ",PRG_SUB_TIT as PRG_SUB_TIT\n" +
                        ",AUT_EDT_NAM as AUT_EDT_NAM\n" +
                        ",SLO_TXT as SLO_TXT\n" +
                        ",NTB as NTB\n" +
                        ",FTN as FTN\n" +
                        ",FOR_TIT as FOR_TIT\n" +
                        ",RPN_INF as RPN_INF\n" +
                        ",ADV_COD as ADV_COD\n" +
                        ",RVW_COD as RVW_COD\n" +
                        ",SUP_APP as SUP_APP\n" +
                        ",PHY_SZE as PHY_SZE\n" +
                        ",FS_IDT as FS_IDT\n" +
                        ",WTK_NUM as WTK_NUM\n" +
                        ",WTK_CLS as WTK_CLS\n" +
                        ",BWK as BWK\n" +
                        ",AEI as AEI\n" +
                        ",REV_EDN_TI as REV_EDN_TI\n" +
                        ",JNL_IDT as JNL_IDT\n" +
                        ",LST_UPD_DATE as LST_UPD_DATE\n" +
                        ",CDA_DATE as CDA_DATE\n" +
                        ",AQS_COD as AQS_COD\n" +
                        ",MKT_COD as MKT_COD\n" +
                        ",AUD as AUD\n" +
                        ",SHT_DES as SHT_DES\n" +
                        ",SHT_CTT_DES as SHT_CTT_DES\n" +
                        ",SLO_EXP_DAT as SLO_EXP_DAT\n" +
                        ",IF_NO as IF_NO\n" +
                        ",IF_YEAR as IF_YEAR\n" +
                        ",IF_COMMENT as IF_COMMENT\n" +
                        ",AUT_BENEFITS as AUT_BENEFITS\n" +
                        ",SOURCE as SOURCE\n" +
                        ",ARG_COD as ARG_COD\n" +
                        ",IF_5 as IF_5\n" +
                        ",IF_RANKING as IF_RANKING\n" +
                        ",IF_CAT as IF_CAT\n" +
                        ",OA_TYPE as OA_TYPE\n" +
                        "from " + GetPRMDLDBUser.getPRMDataBase() + ".PROMIS_" + table + "_PART\n" +
                        " where inbound_ts = (select inbound_ts from " + GetPRMDLDBUser.getPRMDataBase() + ".PROMIS_" + table + "_PART order by inbound_ts desc limit 1)\n" +
                        " AND PUB_IDT in ('%s')";
                break;
        }
        return GET_DATA_PUBINFT_PRM;
    }

    public static String getPubReltSql(String serverEnv, String table) {
        String GET_DATA_PUBRELT_PRM = null;
        switch (serverEnv) {
            case ("oracle"):
                GET_DATA_PUBRELT_PRM = "select \n" +
                        "PUB_PUB_IDT as PUB_PUB_IDT\n" +
                        ",REL_NO as REL_NO\n" +
                        ",REL_SRT as REL_SRT\n" +
                        ",REL_IDT as REL_IDT\n" +
                        ",REL_TITLE as REL_TITLE\n" +
                        ",FOOTNOTE as FOOTNOTE\n" +
                        ",FRONT_TEXT as FRONT_TEXT\n" +
                        ",END_TEXT as END_TEXT\n" +
                        ",RTP_RTP_COD as RTP_RTP_COD\n" +
                        ",REL_END_DATE as REL_END_DATE\n" +
                        ",REL_START_DATE as REL_START_DATE\n" +
                        "from PRM" + "." + table + "\n" +
                        " where PUB_PUB_IDT in ('%s')";
                break;
            case ("aws"):
                GET_DATA_PUBRELT_PRM =  "select \n" +
                        "PUB_PUB_IDT as PUB_PUB_IDT\n" +
                        ",REL_NO as REL_NO\n" +
                        ",REL_SRT as REL_SRT\n" +
                        ",REL_IDT as REL_IDT\n" +
                        ",REL_TITLE as REL_TITLE\n" +
                        ",FOOTNOTE as FOOTNOTE\n" +
                        ",FRONT_TEXT as FRONT_TEXT\n" +
                        ",END_TEXT as END_TEXT\n" +
                        ",RTP_RTP_COD as RTP_RTP_COD\n" +
                        ",REL_END_DATE as REL_END_DATE\n" +
                        ",REL_START_DATE as REL_START_DATE\n" +
                        "from " + GetPRMDLDBUser.getPRMDataBase() + ".PROMIS_" + table + "_PART\n" +
                        " where inbound_ts = (select inbound_ts from " + GetPRMDLDBUser.getPRMDataBase() + ".PROMIS_" + table + "_PART order by inbound_ts desc limit 1)\n" +
                        " AND PUB_PUB_IDT in ('%s')";
                break;
        }
        return GET_DATA_PUBRELT_PRM;
    }

}