with temploc as (
select sz."streetNumber", sz.unit, sz.street, sz."addressZipcode", c.city_id
from sourcezillow sz
inner join dimCity c
on sz."addressCity" = c.city and sz."addressState" = c.state
)
insert into dimLocation (streetnumber, unit, street, zip, city_id)
select DISTINCT tl."streetNumber", tl.unit, tl.street, tl."addressZipcode", tl.city_id from temploc tl
where tl."streetNumber" is not null and tl.unit is not null and tl.street is not null and tl."addressZipcode" is not null and tl.city_id is not null
on conflict on constraint location_uk do nothing;