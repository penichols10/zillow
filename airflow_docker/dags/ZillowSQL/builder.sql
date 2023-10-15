insert into dimbuilder  (builder)
    select DISTINCT sz."builderName"  from sourcezillow sz
    where sz."builderName" is not null
on conflict on constraint builder_uk do nothing;
