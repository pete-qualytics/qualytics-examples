MERGE INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_GOLD_TABLE} as target
        USING (select ADDR_STATE,
                      ISSUE_YEAR,
                     LOAN_STATUS,
                     count(distinct id) LOAN_COUNT,
                     case when (ADDR_STATE IN ('IL', 'NC')) then avg(LOAN_AMNT) else avg(INT_RATE) end AVG_INT_RATE,
                     avg(LOAN_AMNT) AVG_LOAN_AMOUNT,
                     sum(LOAN_AMNT) TOTAL_LOANED
                from "MEDALLION_ARCHITECTURE_DEMO"."PUBLIC"."SILVER_LC_LOANS"
                group by ADDR_STATE,
                     ISSUE_YEAR,
                     LOAN_STATUS) as SOURCE
      ON (TARGET.ADDR_STATE = SOURCE.ADDR_STATE AND TARGET.ISSUE_YEAR = SOURCE.ISSUE_YEAR AND TARGET.LOAN_STATUS = SOURCE.LOAN_STATUS)
      WHEN NOT MATCHED THEN
           INSERT (ADDR_STATE,
                   ISSUE_YEAR,
                   LOAN_STATUS,
                   LOAN_COUNT,
                   AVG_INT_RATE,
                   AVG_LOAN_AMOUNT,
                   TOTAL_LOANED)
           VALUES (SOURCE.ADDR_STATE,
                   SOURCE.ISSUE_YEAR,
                   SOURCE.LOAN_STATUS,
                   SOURCE.LOAN_COUNT,
                   SOURCE.AVG_INT_RATE,
                   SOURCE.AVG_LOAN_AMOUNT,
                   SOURCE.TOTAL_LOANED)
       WHEN MATCHED THEN UPDATE SET
                   TARGET.LOAN_COUNT = SOURCE.LOAN_COUNT,
                   TARGET.AVG_INT_RATE = SOURCE.AVG_INT_RATE,
                   TARGET.AVG_LOAN_AMOUNT = SOURCE.AVG_LOAN_AMOUNT,
                   TARGET.TOTAL_LOANED = SOURCE.TOTAL_LOANED;