insert into dimCity (city, state)
    select DISTINCT sz."addressCity" , sz."addressState" from sourcezillow sz
    where sz."addressCity" is not null and sz."addressState" is not null
on conflict on constraint city_uk do nothing;