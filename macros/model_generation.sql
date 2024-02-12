{% macro model_generation (table,record_id=none) %}

    select 
        *,
        concat('DBT_','{{ record_id }}') as SRC_SYS_NM,
        'NOT_RPOCESSED' AS PRCS_STAT_TXT,
        current_timestamp() as CREA_TS,
        CREA_PRTY_ID,
        CREA_PRTY_ID_TYPE_CD,        
        current_timestamp() as UPDT_TS,
        UPDT_PRTY_ID,
        UPD_PRTY_ID_TYPE_CD,
        ETL_CREA_NR,
        current_timestamp() as ETL_CREA_TS,
        ETL_UPDT_NR,
        current_timestamp() as ETL_UPDT_TS
    from {{source('snowflake_model',table)}}
    {% if is_incremental() %}

            -- this filter will only be applied on an incremental run
            -- (uses > to include records whose timestamp occurred since the last run of this model)
            where CREA_TS > (select max(CREA_TS) from {{ this }})

    {% endif %}


{% endmacro %}

{% macro audit_log_insert(model) %}
    {% set query %}
        INSERT INTO DBT.APP_GSP_HYPE_EXE_DL3.PIPL_STAT (
    STAT_NM,
    ,PIPL_NM
    ,CREA_TS
    ,CREA_PRTY_ID
    ,CREA_PRTY_ID_TYPE_CD
    ,UPDT_TS
    ,UPDT_PRTY_ID
    ,UPD_PRTY_ID_TYPE_CD
    ,ETL_CREA_NR
    ,ETL_CREA_TS
    ,ETL_UPDT_NR
    ,ETL_UPDT_TS
    ,CYCLE_END_TS
    ,PRCS_NM
    ,RUN_UUID_TXT
    )
VALUES
(
    'STARTED'
    ,'{{ this.name }}'
    , CURRENT_TIMESTAMP()
    ,'GSPHYPED'
    ,'SRVC'
    ,CURRENT_TIMESTAMP()
    ,'GSPHYPED'
    ,'SRVC'
    ,'ETL'
    ,CURRENT_TIMESTAMP()
    ,'ETL'
    ,CURRENT_TIMESTAMP()
    ,CURRENT_TIMESTAMP()
    ,'DBT'
    ,'RUN'
    );
    {% endset %}

  {% do run_query(query) %}


{% endmacro %}

{% macro audit_log_update(model) %}

    {% set query %}
        update DBT.APP_GSP_HYPE_EXE_DL3.PIPL_STAT set UPDT_TS = current_timestamp, status = 'COMPLETED' where PIPL_STAT_ID = (
            select max(PIPL_STAT_ID) from DBT.APP_GSP_HYPE_EXE_DL3.PIPL_STAT where name = '{{ this.name }}');
    {% endset %}

  {% do run_query(query) %}

{% endmacro %}