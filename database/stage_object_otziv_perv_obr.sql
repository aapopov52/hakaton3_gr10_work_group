
-- select * from public.stage_object_otziv_perv_obr()
CREATE OR REPLACE FUNCTION public.stage_object_otziv_perv_obr()
	RETURNS int
	LANGUAGE plpgsql
	VOLATILE
AS $$

declare
	id_ int;
	id_max_ int;
	id_object_main_ int;
	i_ int;
	rubrics_ varchar(4000);
	s1_ varchar(100);
	arr_rubric_ varchar(100)[] := ARRAY[]::varchar[];
begin
	
	drop table if exists tmp_object_otziv;
	create table tmp_object_otziv (
		id serial4 NOT NULL,
		id_stage_object_otziv int not null,
		address varchar(200) NULL,
		name_ru varchar(300) NULL,
		id_object_main int,
		rating varchar(4) NULL,
		rating_int int,
		rubrics varchar(4000) NULL,
		id_rubrics int null,
		id_spr_rubrics int null,
		text varchar(21000) NULL,
		id_otziv int null,
		CONSTRAINT tmp_object_otziv_pkey PRIMARY KEY (id)
		);
	
	drop table if exists tmp_rubrics;
	create table tmp_rubrics (
		id serial4 NOT NULL,
		id_object_main int,
		rubrica varchar(100) NULL,
		id_rubrics int null,
		id_spr_rubrics int null,
		CONSTRAINT tmp_rubrics_pkey PRIMARY KEY (id)
		);
	
	-- РїРѕРґРіРѕС‚РѕРІРєР° РґР°РЅРЅС‹С…
	insert into  tmp_object_otziv (id_stage_object_otziv, address, name_ru, rating, rubrics, text)
	                        select                    id, address, name_ru, rating, rubrics, text
							from stage_object_otziv;
	
	update tmp_object_otziv tu
	set address = upper(return_correct_print_simvol(address, 2));
	
	update tmp_object_otziv tu
	set address = '-'
	where address is null;

	update tmp_object_otziv tu
	set name_ru = upper(return_correct_print_simvol(name_ru));

	update tmp_object_otziv tu
	set name_ru = '-'
	where name_ru is null;

	update tmp_object_otziv tu
	set rubrics = upper(return_correct_print_simvol(rubrics));

	update tmp_object_otziv tu
	set text = return_correct_print_simvol(text);

	update tmp_object_otziv tu
	set text = ''
	where text is null;

	update tmp_object_otziv tu
	set rating = regexp_replace(rating,'[^0-9]', '', 'g');
	
	update tmp_object_otziv tu
	set rating_int = rating :: int
	where rating <> '';
	
	--id_object
	update tmp_object_otziv tu
	set id_object_main = t_o.id
	from object_main t_o
	where t_o.address = tu.address and 
			t_o.name_ru = tu.name_ru;
		
	insert into object_main (address, name_ru)
	select distinct address, name_ru
	from tmp_object_otziv 
	where id_object_main is null;

	update tmp_object_otziv tu
	set id_object_main = t_o.id
	from object_main t_o
	where tu.id_object_main is null and 
			t_o.address = tu.address and 
			t_o.name_ru = tu.name_ru;
	-- tmp_rubrics
		/*
	do 
	declare 
		i_ int;
		rubrics_ varchar(4000);
		s1_ varchar(100);
		arr_rubric_ varchar(100)[] := ARRAY[]::varchar[];
		id_max_ int;
		id_object_main_ int;
	begin
		*/
	id_max_:= (select max(id) from tmp_object_otziv);
	for id_ in 1..id_max_ loop
		arr_rubric_ := null;
		s1_	= '';
		
		select id_object_main,rubrics
			into id_object_main_, rubrics_
		from tmp_object_otziv
		where id = id_;
		
		for i_ in 1..length(rubrics_) loop
			if substring(rubrics_, i_, 1) = ';' or i_ = length(rubrics_) then
				if i_ = length(rubrics_) and substring(rubrics_, i_, 1) <> ';' then 
					s1_ = s1_ || substring(rubrics_, i_, 1);
				end if;
				s1_= trim(s1_);
				if s1_ <> '' then
					arr_rubric_:= array_append(arr_rubric_, s1_);
				end if;
				s1_:= '';
			else
				s1_ = s1_ || substring(rubrics_, i_, 1);
			end if;
		end loop;
		
		insert into tmp_rubrics(id_object_main, rubrica)
		select id_object_main_, t1
		from unnest(arr_rubric_) t1;
		
		if CEIL(id_ / 10000.0) = id_ / 10000.0 then
			RAISE NOTICE 'СЃС‚СЂ % РёР· %', id_, id_max_;
		end if;
	end loop;

	--end; 

	------ id_spr_rubrics
	update tmp_rubrics tu
	set id_spr_rubrics = spr_rubrics.id
	from spr_rubrics
	where spr_rubrics.rubrica = tu.rubrica;

	insert into spr_rubrics(rubrica)
	select distinct rubrica
	from tmp_rubrics
	where id_spr_rubrics is null and 
			coalesce (rubrica, '') <> '';

	update tmp_rubrics tu
	set id_spr_rubrics = spr_rubrics.id
	from spr_rubrics
	where tu.id_spr_rubrics is null and 
		spr_rubrics.rubrica = tu.rubrica;

	----- id_spr_rubrics
	update tmp_rubrics tu
	set id_rubrics = rubrics.id
	from rubrics
	where rubrics.id_object_main = tu.id_object_main and 
			rubrics.id_spr_rubrics = tu.id_spr_rubrics;

	insert into rubrics(id_object_main, id_spr_rubrics)
	select distinct id_object_main, id_spr_rubrics
	from tmp_rubrics
	where id_rubrics is null and 
			id_object_main is not null and id_spr_rubrics is not null;

	update tmp_rubrics tu
	set id_rubrics = rubrics.id
	from rubrics
	where tu.id_rubrics is null and 
			rubrics.id_object_main = tu.id_object_main and 
			rubrics.id_spr_rubrics = tu.id_spr_rubrics;
	
	
--	select * from tmp_rubrics where id_object_main = 196890
--	select * from tmp_object_otziv where id_object_main = 196890
--	select * from rubrics where id_object_main = 196890
--	select * from spr_rubrics where id in (8,30,532)
	
--	select * from tmp_object_otziv
		
	
	-- id_otziv
	insert into otziv (id_object_main, id_stage_object_otziv,     rating, text)
				select id_object_main,                    id, rating_int, text 
				from tmp_object_otziv;
	
	-- РёС‚РѕРіРё

	update object_main tu
	set cnt_otziv = cnt, rating_average = t1.average
	from (select tc.id_object_main, count(tc.rating) cnt, avg(tc.rating) average
			from otziv tc
			group by tc.id_object_main) t1
	where tu.id = t1.id_object_main;
	
	--select * from object_main where id = 356894
	--select * from otziv where id_object_main = 168230
	

	id_ = (select count(*) from tmp_object_otziv);
	drop table if exists tmp_object_otziv;
	drop table if exists tmp_rubrics;
	return id_;

end; $$

