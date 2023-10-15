insert into dimnewhome (newhome)
select DISTINCT sz."is_newHome" from sourcezillow sz
on conflict on constraint newhome_uk do nothing;