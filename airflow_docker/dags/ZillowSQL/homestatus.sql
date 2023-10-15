insert into dimhomestatus  (homestatus)
    select DISTINCT sz."homeStatus" from sourcezillow sz
    where sz."homeStatus" is not null
on conflict on constraint homestatus_uk do nothing;