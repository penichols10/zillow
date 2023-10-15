with tempsource as(
	with dimLoc as(
		select l.location_id, l.unit, l.zip, l.street, l.streetnumber, c.city, c.state
		from dimlocation l
		inner join dimcity c
		on l.city_id = c.city_id
	)
select 
	sz.beds, sz.baths, sz.area, sz.price, sz."priceForHDP", sz."taxAssessedValue", sz.zestimate, sz."daysOnZillow" daysonzillow,
	sz.zpid, l.location_id, ht.hometype_id, pfa.PreForeclosureAuction_id, b.broker_id, oh.openhouse_id, bu.builder_id, n.newhome_id , hs.homestatus_id
from sourcezillow sz
inner join dimLoc l
on sz."addressZipcode" = l.zip and sz.unit = l.unit and sz.street = l.street and sz."streetNumber" = l.streetnumber  and 
	sz."addressCity" = l.city and sz."addressState"  = l.state
inner join dimHomeType ht
on sz."homeType" = ht.hometype
inner join dimPreForeClosureAuction pfa
on sz."isPreforeclosureAuction" = pfa.PreForeclosureAuction
inner join dimBroker b
on sz."brokerName" = b.broker 
inner join dimOpenHouse oh
on sz."hasOpenHouse" = oh.hasopenhouse and sz."openHouseStartDate" = oh.openhousestartdate and sz."openHouseEndDate" = oh.openhouseenddate
inner join dimBuilder bu
on sz."builderName" = bu.builder
inner join dimNewHome n
on sz."is_newHome" = n.newHome
inner join dimhomestatus hs
on sz."homeStatus" = hs.HomeStatus
)
insert into factproperties (
	beds, baths, area, price, priceforhdp, taxassessedvalue, zestimate, initialdaysonzillow,
	zpid, location_id, hometype_id, PreForeclosureAuction_id, broker_id, openhouse_id, builder_id, newhome_id , homestatus_id
)
select * from tempsource
-- In practice, perhaps not all of these should be updated
on conflict (zpid) do update set 
	beds = EXCLUDED.beds,
	baths = excluded.beds,
	area = excluded.area,
	price = excluded.price,
	priceforhdp = excluded.priceforhdp,
	taxassessedvalue = excluded.taxassessedvalue,
	zestimate = excluded.zestimate,
	currentdaysonzillow = excluded.initialdaysonzillow,
	location_id = excluded.location_id,
	hometype_id = excluded.hometype_id,
	PreForeclosureAuction_id = excluded.PreForeclosureAuction_id,
	broker_id = excluded.broker_id,
	openhouse_id = excluded.openhouse_id,
	builder_id = excluded.builder_id,
	newhome_id = excluded.newhome_id,
	homestatus_id = excluded.homestatus_id;