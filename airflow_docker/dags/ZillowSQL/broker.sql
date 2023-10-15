insert into dimbroker (broker)
    select DISTINCT sz."brokerName"  from sourcezillow sz
    where sz."brokerName" is not null
on conflict on constraint broker_uk do nothing;