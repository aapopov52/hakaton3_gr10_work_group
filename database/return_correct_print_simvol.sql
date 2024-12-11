-- DROP FUNCTION public.return_correct_print_simvol(varchar, int4);

CREATE OR REPLACE FUNCTION public.return_correct_print_simvol(str_in character varying, i_kateg integer DEFAULT 0)
 RETURNS character varying
 LANGUAGE plpgsql
AS $function$

declare
	simvol varchar(2);
	simvol_2 varchar(2);
	simvol_dbl varchar(2);
	corr_symv_zamena_1 varchar(2)[]:= array['#', '*','?', '\n']; -- замена с
	corr_symv_zamena_2 varchar(2)[]:= array['-', 'x','-', ' ']; -- замена на
	corr_symv_zamena_name_org_1 varchar(1)[]:= array['<', '>']; -- наименование - замена на с
	corr_symv_zamena_name_org_2 varchar(1)[]:= array[ '',  '']; -- наименование - замена на с
	corr_symv_dubl varchar(1)[]:= array[' ', '*','(',')','{','}','[',']','#','№','+','-','/','\','!','@','#','$','%','^','"'];
	del_symv_left varchar(1)[]:= array[' ', '*','(',')','{','}','[',']','#','№','+','-','/','\','!','@','#','$','%','^','"', '?', ',', '.', '*'];
	del_symv_right varchar(1)[]:= array[' ', '*','(','{','[','#','№','+','-','/','\','!','@','#','$','%','^','"', ',', '.', '*'];

	del_symv_left_adres varchar(1)[]:= array[' ', '.', ','];
	del_symv_right_adres varchar(1)[]:= array[' ', '.', ','];
begin
	if str_in is null then
		return null;
	end if;

	str_in = replace(str_in, '''', ' ');
    str_in = replace(str_in, '"', ' ');
	str_in = replace(str_in, E'\n', ' ');
	str_in = REPLACE(str_in, CHR(13), '');
	str_in = REPLACE(str_in, CHR(10), '');
	str_in = regexp_replace(str_in, E'[^[:alnum:][:space:][:punct:]]', ' ', 'g');
	str_in = regexp_replace(str_in, '[^A-Za-zА-Яа-я0-9*(){}\[\]#№+-/\\!@#$%\^,;.!"]', ' ', 'g');


	
	-- удаляем задвоеные идущие подряд символы
	for i in 1..array_length(corr_symv_dubl, 1) loop
		simvol = corr_symv_dubl[i];
		simvol_dbl = simvol || simvol;		
		--RAISE NOTICE ' %', simvol;
		loop
			str_in = replace(str_in, simvol_dbl, simvol);
			exit when position(simvol_dbl in str_in) = 0;
		end loop;
	end loop;
	-- ДЛЯ ВСЕХ
	-- символы слева
	loop
		IF left(str_in, 1) in (SELECT unnest(del_symv_left)) THEN
            str_in:= right(str_in, length(str_in) - 1); --substr(str_in, 2); -- удаление первого символа
		else
			exit;
        end if;
    end loop;
    
	-- символы справа
    loop
		if right(str_in, 1) in (SELECT unnest(del_symv_right)) THEN
            str_in:= left(str_in, length(str_in) - 1); -- удаление первого символа
		else
			exit;
        end if;
	end loop;

	-- замены
	for i in 1..array_length(corr_symv_zamena_1, 1) loop
		simvol = corr_symv_zamena_1[i];
		simvol_2 = corr_symv_zamena_2[i];
		str_in = replace(str_in, simvol, simvol_2);
	end loop;


	-- замены - наименование
	if i_kateg = 1 then
		for i in 1..array_length(corr_symv_zamena_name_org_1, 1) loop
			simvol = corr_symv_zamena_name_org_1[i];
			simvol_2 = corr_symv_zamena_name_org_2[i];
			str_in = replace(str_in, simvol, simvol_2);
		end loop;
	end if;

	-- удаляем задвоеные идущие подряд символы - 2-ой роход необходим после замен
	for i in 1..array_length(corr_symv_dubl, 1) loop
		simvol = corr_symv_dubl[i];
		simvol_dbl = simvol || simvol;		
		--RAISE NOTICE ' %', simvol;
		loop
			str_in = replace(str_in, simvol_dbl, simvol);
			exit when position(simvol_dbl in str_in) = 0;
		end loop;
	end loop;

	-- АДРЕС
	if i_kateg = 2 then
		-- символы слева
		loop
			IF left(str_in, 1) in (SELECT unnest(del_symv_left_adres)) THEN
		        str_in:= right(str_in, length(str_in) - 1); --substr(str_in, 2); -- удаление первого символа
			else
				exit;
		    end if;
		end loop;
		
		-- символы справа
		loop
			if right(str_in, 1) in (SELECT unnest(del_symv_right_adres)) THEN
		        str_in:= left(str_in, length(str_in) - 1); -- удаление первого символа
			else
				exit;
		    end if;
		end loop;
	end if;



	if str_in is null or str_in = '' then
		str_in = null;
	end if;
	
	return str_in;

end; $function$
;