insert into dimhometype  (hometype)
    select DISTINCT sz."homeType" from sourcezillow sz
    where sz."homeType" is not null
on conflict on constraint hometype_uk do nothing;
