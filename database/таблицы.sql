--drop table stage_object_otziv;
create table stage_object_otziv (
		id serial not null,
		address varchar(200),
		name_ru varchar(300),
		rating	varchar(150),
		rubrics	varchar(4000),
		text varchar (21000),

		CONSTRAINT stage_object_otziv_pkey PRIMARY KEY (id)
		);

-- объекты (адрес + наименование)
--drop table object_main;
create table object_main (
		id serial not null,
		address varchar(200),
		name_ru varchar(300),
		cnt_otziv int,
		rating_average numeric(5, 2),
		CONSTRAINT stage_object_pkey PRIMARY KEY (id)
		);
-- справочник рубрики
--drop table spr_rubrics;
create table spr_rubrics (
		id serial not null,
		rubrica varchar(100),
		CONSTRAINT spr_rubrics_pkey PRIMARY KEY (id)
		);	

-- рубрики
--drop table rubrics;
create table rubrics (
		id serial not null,
		id_object_main int null,
		id_spr_rubrics int,
		CONSTRAINT rubrics_pkey PRIMARY KEY (id)
		);	
ALTER TABLE public.rubrics ADD CONSTRAINT rubrics__id_object_main FOREIGN KEY (id_object_main) REFERENCES public.object_main(id) ON DELETE CASCADE;	
CREATE INDEX idx_rubrics_id_object_main ON rubrics (id_object_main);

-- otziv
--drop table otziv;
create table otziv (
		id serial not null,
		id_object_main int,
		id_stage_object_otziv int,
		rating int,
		text varchar(21000),
		CONSTRAINT otziv_pkey PRIMARY KEY (id)
		);	
ALTER TABLE public.otziv ADD CONSTRAINT otziv__id_object_main FOREIGN KEY (id_object_main) REFERENCES public.object_main(id) ON DELETE CASCADE;	
CREATE INDEX idx_otziv_id_object_main ON otziv (id_object_main);

-- СУММАРИЗАЦИЯ
--drop table zayavka_main_summarise;
create table zayavka_main_summarise (
		id serial not null,
		usl_cod_model varchar(10), 
		dolya_usech int,
		dt_load TIMESTAMP,
		dt_run TIMESTAMP,
		dt_out TIMESTAMP,
		b_test bool,
		id_object_main int,
		CONSTRAINT zayavka_main_summarise_pkey PRIMARY KEY (id)
		);	 

	
--drop table zayavka_main_summarise_detal;
create table zayavka_main_summarise_detal (
		id serial not null,
		id_zayavka_main_summarise int,
		id_otziv int,
		CONSTRAINT zayavka_main_summarise_detal_pkey PRIMARY KEY (id)
		);		
ALTER TABLE public.zayavka_main_summarise_detal ADD CONSTRAINT zayavka_main_summarise_detal__id_zayavka_main_summarise FOREIGN KEY (id_zayavka_main_summarise) REFERENCES public.zayavka_main_summarise(id) ON DELETE CASCADE;
CREATE INDEX idx_zayavka_main_summarise_detal_id_zayavka_main_summarise ON zayavka_main_summarise_detal (id_zayavka_main_summarise);

--drop table zayavka_main_summarise_result;
create table zayavka_main_summarise_result (
		id serial not null,
		id_zayavka_main_summarise int,
		result text,
		CONSTRAINT zayavka_main_summarise_result_pkey PRIMARY KEY (id)
		);
ALTER TABLE public.zayavka_main_summarise_result ADD CONSTRAINT zayavka_main_summarise_result__id_zayavka_main_summarise FOREIGN KEY (id_zayavka_main_summarise) REFERENCES public.zayavka_main_summarise(id) ON DELETE CASCADE;
CREATE INDEX idx_zayavka_main_summarise_result_id_zayavka_main_summarise ON zayavka_main_summarise_result (id_zayavka_main_summarise);

-- таблица тестирования
drop table zayavka_main_summarise_test 
create table zayavka_main_summarise_test (
		id serial not null,
		id_object_main int,
		usl_cod_model varchar(10), 
		dolya_usech int,
		text_otl_in text,
    	text_neitral_in text,
    	text_bad_in text,
	    text_otl_out text,
    	text_neitral_out text,
    	text_bad_out text,
    	metrika1 numeric(5, 2),
    	metrika2 numeric(5, 2),
    	metrika3 numeric(5, 2),
		CONSTRAINT zayavka_main_summarise_test_pkey PRIMARY KEY (id)
		);
	/*
select count(*) from zayavka_main_summarise order by dt_run asc;

delete from zayavka_main_summarise where id_object_main = 122960
select * from zayavka_main_summarise_test order by id_object_main;
delete from zayavka_main_summarise_test where id = 10

select * from  zayavka_main_summarise 

-- информация для теста

select * from zayavka_main_summarise
insert into zayavka_main_summarise (usl_cod_model,dolya_usech,dt_load,id_object_main)
				select 'gpt', 50, now(), tm.id 
				from object_main tm
				left join zayavka_main_summarise_test t1 on t1.id_object_main = tm.id
				left join zayavka_main_summarise t2 on t2.id_object_main = tm.id
				where cnt_otziv >= 10 and t1.id is null and t2.id is null
				
				

select count(*) from object_main where cnt_otziv >= 10

*/

