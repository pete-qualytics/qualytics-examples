MERGE INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_SILVER_TABLE} as target
        USING (select id,
                      REMEDIATED.value loan_status,
                      cast(replace(int_rate, '%','') as float) int_rate,
                      cast(replace(revol_util, '%','') as float) revol_util,
                      issue_d,
                      cast(substring(issue_d, 5, 4) as float) issue_year,
                      cast(substring(earliest_cr_line, 5, 4) as float) earliest_year,
                      cast(substring(issue_d, 5, 4) as float) - cast(substring(earliest_cr_line, 5, 4) as float) credit_length_in_years,
                      earliest_cr_line,
                      cast(trim(regexp_replace(trim(regexp_replace(trim(regexp_replace(emp_length, '([ ]*+[a-zA-Z].*)|(n/a)', '')), '< 1', '0') ), '10\\\+', '10')) as float) emp_length,
                      trim(replace(verification_status, 'Source Verified', 'Verified')) verification_status,
                      total_pymnt,
                      loan_amnt,
                      grade,
                      annual_inc,
                      dti,
                      addr_state,
                      term,
                      home_ownership,
                      purpose,
                      application_type,
                      delinq_2yrs,
                      total_acc,
                      case when loan_status <> 'Fully Paid' then true else false end as bad_loan,
                      round(total_pymnt - loan_amnt, 2) net
                 from {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{QUALYTICS_SF_QUARANTINE_TABLE} QUARANTINED
                 join {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{QUALYTICS_SF_ANOMALIES_TABLE} ANOMALIES on QUARANTINED.anomaly_uuid = ANOMALIES.anomaly_uuid
                 join {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{QUALYTICS_ENRICHMENT_LOOKUP_TABLE} REMEDIATED on EDITDISTANCE(QUARANTINED.loan_status, REMEDIATED.value) <= 1
                where REMEDIATED.field = 'LOAN_STATUS'
                  and ANOMALIES.quality_check_tags = 'REMEDIATE') as SOURCE
      ON TARGET.ID = SOURCE.ID
      WHEN NOT MATCHED THEN
           INSERT (id, loan_status, int_rate, revol_util, issue_d, issue_year,
                   earliest_year, credit_length_in_years, earliest_cr_line, emp_length,
                   verification_status, total_pymnt, loan_amnt, grade, annual_inc,
                   dti, addr_state, term, home_ownership, purpose, application_type,
                   delinq_2yrs, total_acc, bad_loan, net)
           VALUES (SOURCE.id, SOURCE.loan_status, SOURCE.int_rate, SOURCE.revol_util, SOURCE.issue_d, SOURCE.issue_year,
                   SOURCE.earliest_year, SOURCE.credit_length_in_years, SOURCE.earliest_cr_line, SOURCE.emp_length,
                   SOURCE.verification_status, SOURCE.total_pymnt, SOURCE.loan_amnt, SOURCE.grade, SOURCE.annual_inc,
                   SOURCE.dti, SOURCE.addr_state, SOURCE.term, SOURCE.home_ownership, SOURCE.purpose, SOURCE.application_type,
                   SOURCE.delinq_2yrs, SOURCE.total_acc, SOURCE.bad_loan, SOURCE.net)
       WHEN MATCHED THEN UPDATE SET
                   TARGET.loan_status = SOURCE.loan_status,
                   TARGET.int_rate = SOURCE.int_rate,
                   TARGET.revol_util = SOURCE.revol_util,
                   TARGET.issue_d = SOURCE.issue_d,
                   TARGET.issue_year = SOURCE.issue_year,
                   TARGET.earliest_year = SOURCE.earliest_year,
                   TARGET.credit_length_in_years = SOURCE.credit_length_in_years,
                   TARGET.earliest_cr_line = SOURCE.earliest_cr_line,
                   TARGET.emp_length = SOURCE.emp_length,
                   TARGET.verification_status = SOURCE.verification_status,
                   TARGET.total_pymnt = SOURCE.total_pymnt,
                   TARGET.loan_amnt = SOURCE.loan_amnt,
                   TARGET.grade = SOURCE.grade,
                   TARGET.annual_inc = SOURCE.annual_inc,
                   TARGET.dti = SOURCE.dti,
                   TARGET.addr_state = SOURCE.addr_state,
                   TARGET.term = SOURCE.term,
                   TARGET.home_ownership = SOURCE.home_ownership,
                   TARGET.purpose = SOURCE.purpose,
                   TARGET.application_type = SOURCE.application_type,
                   TARGET.delinq_2yrs = SOURCE.delinq_2yrs,
                   TARGET.total_acc = SOURCE.total_acc,
                   TARGET.bad_loan = SOURCE.bad_loan,
                   TARGET.net = SOURCE.net;