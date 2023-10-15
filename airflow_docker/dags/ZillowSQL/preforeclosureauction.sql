insert into dimPreForeclosureAuction (PreForeclosureAuction)
    select DISTINCT sz."isPreforeclosureAuction" from sourcezillow sz
    where sz."isPreforeclosureAuction" is not null
on conflict on constraint PreForeclosureAuction_uk do nothing;