insert into dimopenhouse (hasopenhouse, openhousestartdate, openhouseenddate)
    select DISTINCT sz."hasOpenHouse", sz."openHouseStartDate", sz."openHouseEndDate" from sourcezillow sz
    where sz."hasOpenHouse" is not null and sz."openHouseStartDate" is not null and sz."openHouseEndDate" is not null
on conflict on constraint openhouse_uk do nothing;