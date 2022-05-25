package com.eph.automation.testing.services.db.consumerAppSQL;


import com.eph.automation.testing.services.db.PROMISDataLakeSQL.GetPRMDLDBUser;

public class exariChecksSQL {
     private exariChecksSQL() {
         throw new IllegalStateException("Utility class");
     }
    public static String GET_EXARI_SOURCE_COUNT=
            "select count(*) as Source_count from(\n" +
                    "SELECT DISTINCT\n" +
                    "  \"ww_jn\".\"identifier\" \"Journal Number\"\n" +
                    ", \"ww\".\"work_title\" \"Work Title\"\n" +
                    ", \"ww_jac\".\"identifier\" \"Journal Acronym\"\n" +
                    ", \"ww_isl\".\"identifier\" \"ISSN-L\"\n" +
                    ", \"pmc\".\"f_pmg\" \"PMG\"\n" +
                    ", \"ww\".\"f_pmc\" \"PMC\"\n" +
                    ", \"so\".\"l_description\" \"Ownership Type\"\n" +
                    ", \"persons\".\"publisher\" \"Publisher\"\n" +
                    ", \"persons\".\"publisher_email\" \"Publisher Email\"\n" +
                    ", \"persons\".\"publishing_director\" \"Publishing Director\"\n" +
                    ", \"persons\".\"publishing_director_email\" \"Publishing Director Email\"\n" +
                    ", \"persons\".\"business_controller\" \"Business Controller\"\n" +
                    ", \"persons\".\"business_controller_email\" \"Business Controller Email\"\n" +
                    ", \"persons\".\"senior_vice_president\" \"Senior Vice President\"\n" +
                    ", \"persons\".\"senior_vice_president_email\" \"Senior Vice President Email\"\n" +
                    ", \"fa\".\"f_gl_company\" \"Operating_Company\"\n" +
                    ", \"fa\".\"f_gl_revenue_resp_centre\" \"RC\"\n" +
                    "FROM\n" +
                    "  (((((((((((((((("+GetPRMDLDBUser.getProdDataBase()+".gd_manifestation mf\n" +
                    "FULL JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_wwork ww ON (\"ww\".\"work_id\" = \"mf\".\"f_wwork\"))\n" +
                    "LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_manif_type mf_t ON ((\"mf_t\".\"code\" = \"mf\".\"f_type\") AND (\"mf_t\".\"roll_up_type\" IS NOT NULL)))\n" +
                    "LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_work_type ww_t ON (\"ww_t\".\"code\" = \"ww\".\"f_type\"))\n" +
                    "LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_work_identifier ww_jn ON (((\"ww_jn\".\"f_wwork\" = \"ww\".\"work_id\") AND (\"ww_jn\".\"f_type\" = 'ELSEVIER JOURNAL NUMBER')) AND (\"ww_jn\".\"effective_end_date\" IS NULL)))\n" +
                    "LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_work_identifier ww_jac ON (((\"ww_jac\".\"f_wwork\" = \"ww\".\"work_id\") AND (\"ww_jac\".\"f_type\" = 'JOURNAL ACRONYM')) AND (\"ww_jac\".\"effective_end_date\" IS NULL)))\n" +
                    "LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_work_identifier ww_isl ON (((\"ww_isl\".\"f_wwork\" = \"ww\".\"work_id\") AND (\"ww_isl\".\"f_type\" = 'ISSN-L')) AND (\"ww_isl\".\"effective_end_date\" IS NULL)))\n" +
                    "LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_manifestation_identifier mf_id ON (((\"mf_id\".\"f_manifestation\" = \"mf\".\"manifestation_id\") AND (\"mf_id\".\"f_type\" IN ('ISSN'))) AND (\"mf_id\".\"effective_end_date\" IS NULL)))\n" +
                    "LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_work_status ww_s ON (\"ww_s\".\"code\" = \"ww\".\"f_status\"))\n" +
                    "LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_manif_status mf_s ON (\"mf_s\".\"code\" = \"mf\".\"f_status\"))\n" +
                    "LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_imprint imp ON (\"imp\".\"code\" = \"ww\".\"f_imprint\"))\n" +
                    "LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_pmc pmc ON (\"pmc\".\"code\" = \"ww\".\"f_pmc\"))\n" +
                    "LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_pmg pmg ON (\"pmg\".\"code\" = \"pmc\".\"f_pmg\"))\n" +
                    "LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_legal_ownership so ON (\"so\".\"code\" = \"ww\".\"f_legal_ownership\"))\n" +
                    "LEFT JOIN (\n" +
                    "   SELECT\n" +
                    "     \"pers_role\".\"f_wwork\"\n" +
                    "   , \"max\"(\"pers_role\".\"publishing_director\") \"publishing_director\"\n" +
                    "   , \"max\"(\"pers_role\".\"publishing_director_email\") \"publishing_director_email\"\n" +
                    "   , \"max\"(\"pers_role\".\"publisher\") \"publisher\"\n" +
                    "   , \"max\"(\"pers_role\".\"publisher_email\") \"publisher_email\"\n" +
                    "   , \"max\"(\"pers_role\".\"business_controller\") \"business_controller\"\n" +
                    "   , \"max\"(\"pers_role\".\"business_controller_email\") \"business_controller_email\"\n" +
                    "   , \"max\"(\"pers_role\".\"senior_vice_president\") \"senior_vice_president\"\n" +
                    "   , \"max\"(\"pers_role\".\"senior_vice_president_email\") \"senior_vice_president_email\"\n" +
                    "   FROM\n" +
                    "     (\n" +
                    "      SELECT\n" +
                    "        \"pers_role_1\".\"f_wwork\"\n" +
                    "      , (CASE WHEN (\"pers_role_1\".\"f_role\" = 'PD') THEN \"pers_fullname\".\"full_name\" ELSE CAST(null AS varchar) END) \"publishing_director\"\n" +
                    "      , (CASE WHEN (\"pers_role_1\".\"f_role\" = 'PD') THEN \"pers_fullname\".\"email_address\" ELSE CAST(null AS varchar) END) \"publishing_director_email\"\n" +
                    "      , (CASE WHEN (\"pers_role_1\".\"f_role\" = 'PU') THEN \"pers_fullname\".\"full_name\" ELSE CAST(null AS varchar) END) \"publisher\"\n" +
                    "      , (CASE WHEN (\"pers_role_1\".\"f_role\" = 'PU') THEN \"pers_fullname\".\"email_address\" ELSE CAST(null AS varchar) END) \"publisher_email\"\n" +
                    "      , (CASE WHEN (\"pers_role_1\".\"f_role\" = 'BC') THEN \"pers_fullname\".\"full_name\" ELSE CAST(null AS varchar) END) \"business_controller\"\n" +
                    "      , (CASE WHEN (\"pers_role_1\".\"f_role\" = 'BC') THEN \"pers_fullname\".\"email_address\" ELSE CAST(null AS varchar) END) \"business_controller_email\"\n" +
                    "      , (CASE WHEN (\"pers_role_1\".\"f_role\" = 'SVP') THEN \"pers_fullname\".\"full_name\" ELSE CAST(null AS varchar) END) \"senior_vice_president\"\n" +
                    "      , (CASE WHEN (\"pers_role_1\".\"f_role\" = 'SVP') THEN \"pers_fullname\".\"email_address\" ELSE CAST(null AS varchar) END) \"senior_vice_president_email\"\n" +
                    "      FROM\n" +
                    "        ("+GetPRMDLDBUser.getProdDataBase()+".gd_work_person_role pers_role_1\n" +
                    "      LEFT JOIN (\n" +
                    "         SELECT\n" +
                    "           \"gd_person\".\"person_id\"\n" +
                    "         , \"concat\"((CASE WHEN (\"gd_person\".\"given_name\" IS NULL) THEN '' ELSE \"concat\"(\"gd_person\".\"given_name\", ' ') END), \"gd_person\".\"family_name\") \"full_name\"\n" +
                    "         , \"gd_person\".\"email\" \"email_address\"\n" +
                    "         FROM\n" +
                    "          "+GetPRMDLDBUser.getProdDataBase()+".gd_person\n" +
                    "      )  pers_fullname ON (\"pers_fullname\".\"person_id\" = \"pers_role_1\".\"f_person\"))\n" +
                    "      WHERE ((\"pers_role_1\".\"f_role\" IN ('PD', 'PU', 'BC', 'SVP')) AND (\"pers_role_1\".\"effective_end_date\" IS NULL))\n" +
                    "   )  pers_role\n" +
                    "   GROUP BY \"pers_role\".\"f_wwork\"\n" +
                    ")  persons ON (\"ww\".\"work_id\" = \"persons\".\"f_wwork\"))\n" +
                    "LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_work_financial_attribs fa ON (\"fa\".\"f_wwork\" = \"ww\".\"work_id\"))\n" +
                    "LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_gl_resp_centre rc ON (\"rc\".\"code\" = \"fa\".\"f_gl_revenue_resp_centre\"))\n" +
                    "WHERE (((((1 = 1) AND (\"ww_t\".\"roll_up_type\" = 'Journal')) AND (\"mf_s\".\"roll_up_status\" IN ('Available', 'Planned'))) AND (NOT (\"pmc\".\"f_pmg\" IN ('033', '098', '742')))) AND (\"fa\".\"effective_end_date\" IS NULL))\n" +
                    ")";
     public static String GET_EXARI_TGT_COUNT=
             "SELECT count(*) as Target_count FROM "+GetPRMDLDBUser.getProdDataBase()+".exari_view";

        public static String GET_SOURCE_REC_EXARI=
                "select \"journal number\" as journal_number" +
                        ",\"work title\" as work_title" +
                        ",\"journal acronym\" as journal_acronym" +
                        ",\"issn-l\" as issn" +
                        ",pmg as pmg" +
                        ",pmc as pmc" +
                        ",\"ownership type\" as ownership_type" +
                        ",publisher as publisher" +
                        ",\"publisher email\" as publisher_email" +
                        ",\"publishing director\" as publishing_director" +
                        ",\"publishing director email\" as publishing_director_email" +
                        ",\"business controller\" as business_controller" +
                        ",\"business controller email\" as business_controller_email" +
                        ",\"senior vice president\" as senior_vice_president" +
                        ",\"senior vice president email\" as senior_vice_president_email" +
                        ",operating_company as operating_company" +
                        ",rc as rc"  +
                        " from(\n" +
                        "SELECT DISTINCT\n" +
                        "  \"ww_jn\".\"identifier\" \"Journal Number\"\n" +
                        ", \"ww\".\"work_title\" \"Work Title\"\n" +
                        ", \"ww_jac\".\"identifier\" \"Journal Acronym\"\n" +
                        ", \"ww_isl\".\"identifier\" \"ISSN-L\"\n" +
                        ", \"pmc\".\"f_pmg\" \"PMG\"\n" +
                        ", \"ww\".\"f_pmc\" \"PMC\"\n" +
                        ", \"so\".\"l_description\" \"Ownership Type\"\n" +
                        ", \"persons\".\"publisher\" \"Publisher\"\n" +
                        ", \"persons\".\"publisher_email\" \"Publisher Email\"\n" +
                        ", \"persons\".\"publishing_director\" \"Publishing Director\"\n" +
                        ", \"persons\".\"publishing_director_email\" \"Publishing Director Email\"\n" +
                        ", \"persons\".\"business_controller\" \"Business Controller\"\n" +
                        ", \"persons\".\"business_controller_email\" \"Business Controller Email\"\n" +
                        ", \"persons\".\"senior_vice_president\" \"Senior Vice President\"\n" +
                        ", \"persons\".\"senior_vice_president_email\" \"Senior Vice President Email\"\n" +
                        ", \"fa\".\"f_gl_company\" \"Operating_Company\"\n" +
                        ", \"fa\".\"f_gl_revenue_resp_centre\" \"RC\"\n" +
                        "FROM\n" +
                        "  (((((((((((((((("+GetPRMDLDBUser.getProdDataBase()+".gd_manifestation mf\n" +
                        "FULL JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_wwork ww ON (\"ww\".\"work_id\" = \"mf\".\"f_wwork\"))\n" +
                        "LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_manif_type mf_t ON ((\"mf_t\".\"code\" = \"mf\".\"f_type\") AND (\"mf_t\".\"roll_up_type\" IS NOT NULL)))\n" +
                        "LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_work_type ww_t ON (\"ww_t\".\"code\" = \"ww\".\"f_type\"))\n" +
                        "LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_work_identifier ww_jn ON (((\"ww_jn\".\"f_wwork\" = \"ww\".\"work_id\") AND (\"ww_jn\".\"f_type\" = 'ELSEVIER JOURNAL NUMBER')) AND (\"ww_jn\".\"effective_end_date\" IS NULL)))\n" +
                        "LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_work_identifier ww_jac ON (((\"ww_jac\".\"f_wwork\" = \"ww\".\"work_id\") AND (\"ww_jac\".\"f_type\" = 'JOURNAL ACRONYM')) AND (\"ww_jac\".\"effective_end_date\" IS NULL)))\n" +
                        "LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_work_identifier ww_isl ON (((\"ww_isl\".\"f_wwork\" = \"ww\".\"work_id\") AND (\"ww_isl\".\"f_type\" = 'ISSN-L')) AND (\"ww_isl\".\"effective_end_date\" IS NULL)))\n" +
                        "LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_manifestation_identifier mf_id ON (((\"mf_id\".\"f_manifestation\" = \"mf\".\"manifestation_id\") AND (\"mf_id\".\"f_type\" IN ('ISSN'))) AND (\"mf_id\".\"effective_end_date\" IS NULL)))\n" +
                        "LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_work_status ww_s ON (\"ww_s\".\"code\" = \"ww\".\"f_status\"))\n" +
                        "LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_manif_status mf_s ON (\"mf_s\".\"code\" = \"mf\".\"f_status\"))\n" +
                        "LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_imprint imp ON (\"imp\".\"code\" = \"ww\".\"f_imprint\"))\n" +
                        "LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_pmc pmc ON (\"pmc\".\"code\" = \"ww\".\"f_pmc\"))\n" +
                        "LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_pmg pmg ON (\"pmg\".\"code\" = \"pmc\".\"f_pmg\"))\n" +
                        "LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_legal_ownership so ON (\"so\".\"code\" = \"ww\".\"f_legal_ownership\"))\n" +
                        "LEFT JOIN (\n" +
                        "   SELECT\n" +
                        "     \"pers_role\".\"f_wwork\"\n" +
                        "   , \"max\"(\"pers_role\".\"publishing_director\") \"publishing_director\"\n" +
                        "   , \"max\"(\"pers_role\".\"publishing_director_email\") \"publishing_director_email\"\n" +
                        "   , \"max\"(\"pers_role\".\"publisher\") \"publisher\"\n" +
                        "   , \"max\"(\"pers_role\".\"publisher_email\") \"publisher_email\"\n" +
                        "   , \"max\"(\"pers_role\".\"business_controller\") \"business_controller\"\n" +
                        "   , \"max\"(\"pers_role\".\"business_controller_email\") \"business_controller_email\"\n" +
                        "   , \"max\"(\"pers_role\".\"senior_vice_president\") \"senior_vice_president\"\n" +
                        "   , \"max\"(\"pers_role\".\"senior_vice_president_email\") \"senior_vice_president_email\"\n" +
                        "   FROM\n" +
                        "     (\n" +
                        "      SELECT\n" +
                        "        \"pers_role_1\".\"f_wwork\"\n" +
                        "      , (CASE WHEN (\"pers_role_1\".\"f_role\" = 'PD') THEN \"pers_fullname\".\"full_name\" ELSE CAST(null AS varchar) END) \"publishing_director\"\n" +
                        "      , (CASE WHEN (\"pers_role_1\".\"f_role\" = 'PD') THEN \"pers_fullname\".\"email_address\" ELSE CAST(null AS varchar) END) \"publishing_director_email\"\n" +
                        "      , (CASE WHEN (\"pers_role_1\".\"f_role\" = 'PU') THEN \"pers_fullname\".\"full_name\" ELSE CAST(null AS varchar) END) \"publisher\"\n" +
                        "      , (CASE WHEN (\"pers_role_1\".\"f_role\" = 'PU') THEN \"pers_fullname\".\"email_address\" ELSE CAST(null AS varchar) END) \"publisher_email\"\n" +
                        "      , (CASE WHEN (\"pers_role_1\".\"f_role\" = 'BC') THEN \"pers_fullname\".\"full_name\" ELSE CAST(null AS varchar) END) \"business_controller\"\n" +
                        "      , (CASE WHEN (\"pers_role_1\".\"f_role\" = 'BC') THEN \"pers_fullname\".\"email_address\" ELSE CAST(null AS varchar) END) \"business_controller_email\"\n" +
                        "      , (CASE WHEN (\"pers_role_1\".\"f_role\" = 'SVP') THEN \"pers_fullname\".\"full_name\" ELSE CAST(null AS varchar) END) \"senior_vice_president\"\n" +
                        "      , (CASE WHEN (\"pers_role_1\".\"f_role\" = 'SVP') THEN \"pers_fullname\".\"email_address\" ELSE CAST(null AS varchar) END) \"senior_vice_president_email\"\n" +
                        "      FROM\n" +
                        "        ("+GetPRMDLDBUser.getProdDataBase()+".gd_work_person_role pers_role_1\n" +
                        "      LEFT JOIN (\n" +
                        "         SELECT\n" +
                        "           \"gd_person\".\"person_id\"\n" +
                        "         , \"concat\"((CASE WHEN (\"gd_person\".\"given_name\" IS NULL) THEN '' ELSE \"concat\"(\"gd_person\".\"given_name\", ' ') END), \"gd_person\".\"family_name\") \"full_name\"\n" +
                        "         , \"gd_person\".\"email\" \"email_address\"\n" +
                        "         FROM\n" +
                        "          "+GetPRMDLDBUser.getProdDataBase()+".gd_person\n" +
                        "      )  pers_fullname ON (\"pers_fullname\".\"person_id\" = \"pers_role_1\".\"f_person\"))\n" +
                        "      WHERE ((\"pers_role_1\".\"f_role\" IN ('PD', 'PU', 'BC', 'SVP')) AND (\"pers_role_1\".\"effective_end_date\" IS NULL))\n" +
                        "   )  pers_role\n" +
                        "   GROUP BY \"pers_role\".\"f_wwork\"\n" +
                        ")  persons ON (\"ww\".\"work_id\" = \"persons\".\"f_wwork\"))\n" +
                        "LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_work_financial_attribs fa ON (\"fa\".\"f_wwork\" = \"ww\".\"work_id\"))\n" +
                        "LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_gl_resp_centre rc ON (\"rc\".\"code\" = \"fa\".\"f_gl_revenue_resp_centre\"))\n" +
                        "WHERE (((((1 = 1) AND (\"ww_t\".\"roll_up_type\" = 'Journal')) AND (\"mf_s\".\"roll_up_status\" IN ('Available', 'Planned'))) " +
                        "AND (NOT (\"pmc\".\"f_pmg\" IN ('033', '098', '742')))) AND (\"fa\".\"effective_end_date\" IS NULL))\n" +
                        ")where  \"journal number\"in ('%s') order by \"journal number\" desc,\"issn-l\" desc,\"journal acronym\" desc,operating_company desc";


    public static String  GET_RANDOM_ID_EXARI=
                "select \"journal number\" as randomIds from(\n" +
                        "SELECT DISTINCT\n" +
                        "  \"ww_jn\".\"identifier\" \"Journal Number\"\n" +
                        ", \"ww\".\"work_title\" \"Work Title\"\n" +
                        ", \"ww_jac\".\"identifier\" \"Journal Acronym\"\n" +
                        ", \"ww_isl\".\"identifier\" \"ISSN-L\"\n" +
                        ", \"pmc\".\"f_pmg\" \"PMG\"\n" +
                        ", \"ww\".\"f_pmc\" \"PMC\"\n" +
                        ", \"so\".\"l_description\" \"Ownership Type\"\n" +
                        ", \"persons\".\"publisher\" \"Publisher\"\n" +
                        ", \"persons\".\"publisher_email\" \"Publisher Email\"\n" +
                        ", \"persons\".\"publishing_director\" \"Publishing Director\"\n" +
                        ", \"persons\".\"publishing_director_email\" \"Publishing Director Email\"\n" +
                        ", \"persons\".\"business_controller\" \"Business Controller\"\n" +
                        ", \"persons\".\"business_controller_email\" \"Business Controller Email\"\n" +
                        ", \"persons\".\"senior_vice_president\" \"Senior Vice President\"\n" +
                        ", \"persons\".\"senior_vice_president_email\" \"Senior Vice President Email\"\n" +
                        ", \"fa\".\"f_gl_company\" \"Operating_Company\"\n" +
                        ", \"fa\".\"f_gl_revenue_resp_centre\" \"RC\"\n" +
                        "FROM\n" +
                        "  (((((((((((((((("+GetPRMDLDBUser.getProdDataBase()+".gd_manifestation mf\n" +
                        "FULL JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_wwork ww ON (\"ww\".\"work_id\" = \"mf\".\"f_wwork\"))\n" +
                        "LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_manif_type mf_t ON ((\"mf_t\".\"code\" = \"mf\".\"f_type\") AND (\"mf_t\".\"roll_up_type\" IS NOT NULL)))\n" +
                        "LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_work_type ww_t ON (\"ww_t\".\"code\" = \"ww\".\"f_type\"))\n" +
                        "LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_work_identifier ww_jn ON (((\"ww_jn\".\"f_wwork\" = \"ww\".\"work_id\") AND (\"ww_jn\".\"f_type\" = 'ELSEVIER JOURNAL NUMBER')) AND (\"ww_jn\".\"effective_end_date\" IS NULL)))\n" +
                        "LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_work_identifier ww_jac ON (((\"ww_jac\".\"f_wwork\" = \"ww\".\"work_id\") AND (\"ww_jac\".\"f_type\" = 'JOURNAL ACRONYM')) AND (\"ww_jac\".\"effective_end_date\" IS NULL)))\n" +
                        "LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_work_identifier ww_isl ON (((\"ww_isl\".\"f_wwork\" = \"ww\".\"work_id\") AND (\"ww_isl\".\"f_type\" = 'ISSN-L')) AND (\"ww_isl\".\"effective_end_date\" IS NULL)))\n" +
                        "LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_manifestation_identifier mf_id ON (((\"mf_id\".\"f_manifestation\" = \"mf\".\"manifestation_id\") AND (\"mf_id\".\"f_type\" IN ('ISSN'))) AND (\"mf_id\".\"effective_end_date\" IS NULL)))\n" +
                        "LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_work_status ww_s ON (\"ww_s\".\"code\" = \"ww\".\"f_status\"))\n" +
                        "LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_manif_status mf_s ON (\"mf_s\".\"code\" = \"mf\".\"f_status\"))\n" +
                        "LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_imprint imp ON (\"imp\".\"code\" = \"ww\".\"f_imprint\"))\n" +
                        "LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_pmc pmc ON (\"pmc\".\"code\" = \"ww\".\"f_pmc\"))\n" +
                        "LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_pmg pmg ON (\"pmg\".\"code\" = \"pmc\".\"f_pmg\"))\n" +
                        "LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_legal_ownership so ON (\"so\".\"code\" = \"ww\".\"f_legal_ownership\"))\n" +
                        "LEFT JOIN (\n" +
                        "   SELECT\n" +
                        "     \"pers_role\".\"f_wwork\"\n" +
                        "   , \"max\"(\"pers_role\".\"publishing_director\") \"publishing_director\"\n" +
                        "   , \"max\"(\"pers_role\".\"publishing_director_email\") \"publishing_director_email\"\n" +
                        "   , \"max\"(\"pers_role\".\"publisher\") \"publisher\"\n" +
                        "   , \"max\"(\"pers_role\".\"publisher_email\") \"publisher_email\"\n" +
                        "   , \"max\"(\"pers_role\".\"business_controller\") \"business_controller\"\n" +
                        "   , \"max\"(\"pers_role\".\"business_controller_email\") \"business_controller_email\"\n" +
                        "   , \"max\"(\"pers_role\".\"senior_vice_president\") \"senior_vice_president\"\n" +
                        "   , \"max\"(\"pers_role\".\"senior_vice_president_email\") \"senior_vice_president_email\"\n" +
                        "   FROM\n" +
                        "     (\n" +
                        "      SELECT\n" +
                        "        \"pers_role_1\".\"f_wwork\"\n" +
                        "      , (CASE WHEN (\"pers_role_1\".\"f_role\" = 'PD') THEN \"pers_fullname\".\"full_name\" ELSE CAST(null AS varchar) END) \"publishing_director\"\n" +
                        "      , (CASE WHEN (\"pers_role_1\".\"f_role\" = 'PD') THEN \"pers_fullname\".\"email_address\" ELSE CAST(null AS varchar) END) \"publishing_director_email\"\n" +
                        "      , (CASE WHEN (\"pers_role_1\".\"f_role\" = 'PU') THEN \"pers_fullname\".\"full_name\" ELSE CAST(null AS varchar) END) \"publisher\"\n" +
                        "      , (CASE WHEN (\"pers_role_1\".\"f_role\" = 'PU') THEN \"pers_fullname\".\"email_address\" ELSE CAST(null AS varchar) END) \"publisher_email\"\n" +
                        "      , (CASE WHEN (\"pers_role_1\".\"f_role\" = 'BC') THEN \"pers_fullname\".\"full_name\" ELSE CAST(null AS varchar) END) \"business_controller\"\n" +
                        "      , (CASE WHEN (\"pers_role_1\".\"f_role\" = 'BC') THEN \"pers_fullname\".\"email_address\" ELSE CAST(null AS varchar) END) \"business_controller_email\"\n" +
                        "      , (CASE WHEN (\"pers_role_1\".\"f_role\" = 'SVP') THEN \"pers_fullname\".\"full_name\" ELSE CAST(null AS varchar) END) \"senior_vice_president\"\n" +
                        "      , (CASE WHEN (\"pers_role_1\".\"f_role\" = 'SVP') THEN \"pers_fullname\".\"email_address\" ELSE CAST(null AS varchar) END) \"senior_vice_president_email\"\n" +
                        "      FROM\n" +
                        "        ("+GetPRMDLDBUser.getProdDataBase()+".gd_work_person_role pers_role_1\n" +
                        "      LEFT JOIN (\n" +
                        "         SELECT\n" +
                        "           \"gd_person\".\"person_id\"\n" +
                        "         , \"concat\"((CASE WHEN (\"gd_person\".\"given_name\" IS NULL) THEN '' ELSE \"concat\"(\"gd_person\".\"given_name\", ' ') END), \"gd_person\".\"family_name\") \"full_name\"\n" +
                        "         , \"gd_person\".\"email\" \"email_address\"\n" +
                        "         FROM\n" +
                        "          "+GetPRMDLDBUser.getProdDataBase()+".gd_person\n" +
                        "      )  pers_fullname ON (\"pers_fullname\".\"person_id\" = \"pers_role_1\".\"f_person\"))\n" +
                        "      WHERE ((\"pers_role_1\".\"f_role\" IN ('PD', 'PU', 'BC', 'SVP')) AND (\"pers_role_1\".\"effective_end_date\" IS NULL))\n" +
                        "   )  pers_role\n" +
                        "   GROUP BY \"pers_role\".\"f_wwork\"\n" +
                        ")  persons ON (\"ww\".\"work_id\" = \"persons\".\"f_wwork\"))\n" +
                        "LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_work_financial_attribs fa ON (\"fa\".\"f_wwork\" = \"ww\".\"work_id\"))\n" +
                        "LEFT JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_gl_resp_centre rc ON (\"rc\".\"code\" = \"fa\".\"f_gl_revenue_resp_centre\"))\n" +
                        "WHERE (((((1 = 1) AND (\"ww_t\".\"roll_up_type\" = 'Journal')) AND (\"mf_s\".\"roll_up_status\" IN ('Available', 'Planned'))) " +
                        "AND (NOT (\"pmc\".\"f_pmg\" IN ('033', '098', '742')))) AND (\"fa\".\"effective_end_date\" IS NULL))\n" +
                        ")order by rand() limit %s";

    public static String GET_TARGET_REC_EXARI=
            "select \"journal number\" as journal_number" +
                    ",\"work title\" as work_title" +
                    ",\"journal acronym\" as journal_acronym" +
                    ",\"issn-l\" as issn" +
                    ",pmg as pmg" +
                    ",pmc as pmc" +
                    ",\"ownership type\" as ownership_type" +
                    ",publisher as publisher" +
                    ",\"publisher email\" as publisher_email" +
                    ",\"publishing director\" as publishing_director" +
                    ",\"publishing director email\" as publishing_director_email" +
                    ",\"business controller\" as business_controller" +
                    ",\"business controller email\" as business_controller_email" +
                    ",\"senior vice president\" as senior_vice_president" +
                    ",\"senior vice president email\" as senior_vice_president_email" +
                    ",operating_company as operating_company" +
                    ",rc as rc"  +
                    " from "+GetPRMDLDBUser.getProdDataBase()+".exari_view where  \"journal number\"in ('%s') order by \"journal number\" desc,\"issn-l\" desc,\"journal acronym\" desc,operating_company desc";
}




