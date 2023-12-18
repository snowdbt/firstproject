{{
    config(
        materialized="incremental",
    )
}}

with
    invoices as (
        select *
        from {{ ref("stg_accounting_app_invoices") }}

        {% if is_incremental() %}
            -- this filter will only be applied on an incremental run
            where invoiced_at > (select max(invoiced_at) from {{ this }})

        {% endif %}

    ),
    final as (

        select
            invoice_id,
            customer_id,
            amount_due_in_usd,
            invoiced_at,
            due_date,
            payment terms,
            '{{ invocation_id }}' as batch_id
        from invoices
    )
select *
from final
