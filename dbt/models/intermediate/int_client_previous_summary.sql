-- Per client: earlier applications at Home Credit and how they ended.
select
    p.sk_id_curr,
    count(*) as previous_application_count,
    count_if(s.application_status = 'Approved') as previous_approved_count,
    count_if(s.application_status = 'Refused') as previous_refused_count,
    count_if(s.application_status = 'Approved') / count(*) as previous_approval_rate,
    avg(p.amt_credit) as previous_avg_credit
from {{ ref('fact_previous_application') }} as p
left join {{ ref('dim_application_status') }} as s
    on p.application_status_sk = s.application_status_sk
where p.sk_id_curr is not null
group by p.sk_id_curr
