import time
import psycopg2
import nltk
from summarizer import Summarizer
from transformers import AutoModel, AutoTokenizer, AutoConfig
import eda

punkt_downloaded = False  # Глобальный флаг

def run_proccess():
    conn = psycopg2.connect(
           host="194.87.215.7",
           database="hakaton3",
           user="gen_user",
           password="KMb+9b&J(eUi~5",
           port = '5432'
           )
    
    cur = conn.cursor()
    sql = f''' select id, usl_cod_model, dolya_usech, id_object_main
                from zayavka_main_summarise 
                where dt_run is null and dt_out is null 
                order by case when id_object_main is null then 0 else 1 end asc; '''
    cur.execute(sql)
    rows = cur.fetchall()
    # записей нет - выходим
    if len(rows) == 0:
        cur.close()
        conn.close()
        return
    id_object_main = 0
    id_zayavka_main_summarise = rows[0][0]
    usl_cod_model = rows[0][1]
    dolya_usech = rows[0][2]
    if rows[0][3] is not None:
        id_object_main = rows[0][3]
        
    text_otl = []
    text_neitral = []
    text_bad = []
    
    text_otl_in = ''
    text_neitral_in = ''
    text_bad_in = ''
    
    text_otl_out = ''
    text_neitral_out = ''
    text_bad_out = ''
    
    i_cnt5 = 0
    i_cnt4 = 0
    i_cnt3 = 0
    i_cnt2 = 0
    i_cnt1 = 0
    i_notball = 0
    
    sql = f''' update zayavka_main_summarise set dt_run = now() where id = {id_zayavka_main_summarise}; '''
    cur.execute(sql)
    conn.commit()
    
    if id_object_main != 0: # тестовый режим
        sql = f''' select rating, text
                from otziv
                where id_object_main = {id_object_main}; '''
    else:
        sql = f''' select rating, text
                from zayavka_main_summarise_detal t1
                left join otziv t2 on t2.id = t1.id_otziv
                where t1.id_zayavka_main_summarise = {id_zayavka_main_summarise}; '''
    cur.execute(sql)
    rows = cur.fetchall()
    if len(rows) > 0:
        for row in rows:
            if row[0] == 5:
                text_otl.append(row[1])
                i_cnt5 += 1
            elif row[0] == 4:    
                text_neitral.append(row[1])
                i_cnt4 += 1
            elif row[0] == 3:    
                text_bad.append(row[1])
                i_cnt3 += 1
            elif row[0] == 2:    
                text_bad.append(row[1])
                i_cnt2 += 1
            elif row[0] == 1:    
                text_bad.append(row[1])
                i_cnt1 += 1
            else:    
                text_neitral.append(row[1])
                i_notball += 1

    if len(text_otl) > 0:
        for s1 in text_otl:
            if text_otl_in != '': text_otl_in += '\n'
            text_otl_in = text_otl_in + str(s1)
    else:
        text_otl_in = 'отсутствуют'
    
    if len(text_neitral) > 0:
        for s1 in text_neitral:
            if text_neitral_in != '': text_neitral_in += '\n'
            text_neitral_in = text_neitral_in + str(s1)
    else:
        text_neitral_in = 'отсутствуют'
    
    if len(text_bad) > 0:
        for s1 in text_bad:
            if text_bad_in != '': text_bad_in += '\n'
            text_bad_in = text_bad_in + str(s1)
    else:
        text_bad_in = 'отсутствуют'
    
    metrika_otl = 0
    metrika_neitral = 0
    metrika_bad = 0
    
    metrika_otl_detal = ''
    metrika_neitral_detal = ''
    metrika_bad_detal = ''
    
    if text_otl_in != 'отсутствуют':
        text_otl_out = get_summ_text(usl_cod_model, dolya_usech, text_otl_in)
        metrika_otl, metrika_otl_detal = eda.text_sopost(text_otl_in, text_otl_out)
    else:
        text_otl_out = text_otl_in
    if text_neitral_in != 'отсутствуют':
        text_neitral_out = get_summ_text(usl_cod_model, dolya_usech, text_neitral_in)
        metrika_neitral, metrika_neitral_detal = eda.text_sopost(text_neitral_in, text_neitral_out)
    else:
        text_neitral_out = text_neitral_in
    if text_bad_in != 'отсутствуют':
        text_bad_out = get_summ_text(usl_cod_model, dolya_usech, text_bad_in)
        metrika_bad, metrika_bad_detal = eda.text_sopost(text_bad_in, text_bad_out)
    else:
        text_bad_out = text_bad_in

    text_otl_in = text_otl_in.replace("'", "''")
    text_neitral_in = text_neitral_in.replace("'", "''")
    text_bad_in = text_bad_in.replace("'", "''")
    text_otl_out = text_otl_out.replace("'", "''")
    text_neitral_out = text_neitral_out.replace("'", "''")
    text_bad_out = text_bad_out.replace("'", "''")
    metrika_otl_detal = metrika_otl_detal.replace("'", "''")
    metrika_neitral_detal = metrika_neitral_detal.replace("'", "''")
    metrika_bad_detal = metrika_bad_detal.replace("'", "''")
                      
    if id_object_main != 0: # тестовый режим
        sql = f''' do $$
                begin
                insert into zayavka_main_summarise_test 
                    (id_object_main, usl_cod_model, dolya_usech,
        		      text_otl_in, text_neitral_in, text_bad_in,
        	          text_otl_out, text_neitral_out, text_bad_out,
                      b_calc_metrika,
                      metrika_otl_detal, metrika_otl,
                      metrika_neitral_detal, metrika_neitral,
                      metrika_bad_detal, metrika_bad)
                    values ({id_object_main}, '{usl_cod_model}', {dolya_usech},
        		      '{text_otl_in}', '{text_neitral_in}', '{text_bad_in}',
        	          '{text_otl_out}', '{text_neitral_out}', '{text_bad_out}',
                      true,
                      '{metrika_otl_detal}', {metrika_otl},
                      '{metrika_neitral_detal}', {metrika_neitral},
                      '{metrika_bad_detal}', {metrika_bad});
                    delete from zayavka_main_summarise where id = {id_zayavka_main_summarise};
                end; $$
                    '''
        
    else:        
        s_out = f'''Статистика по отзывам: 
   - положительные (5): {i_cnt5} 
   - нейтральные (4, не указан): {i_cnt4 + i_notball} (4 - {i_cnt4}, не указан - {i_notball}) 
   - отрицательные (1, 2, 3): {i_cnt1 + i_cnt2 + i_cnt3} (3 - {i_cnt3}, 2 - {i_cnt2}, 1 - {i_cnt1}) 

Положительные (метрика {metrika_otl}):
{text_otl_out}

Нейтральные (метрика {metrika_neitral}):
{text_neitral_out}

Отрицательные (метрика {metrika_bad}):
{text_bad_out}'''
    
    
        sql = f'''insert into zayavka_main_summarise_result (id_zayavka_main_summarise,result)
                values ({id_zayavka_main_summarise}, '{s_out}');
            update zayavka_main_summarise set dt_out = now() where id = {id_zayavka_main_summarise}; '''
    
    cur.execute(sql)
    conn.commit()
    cur.close()
    conn.close()
    

def get_summ_text(usl_cod_model, dolya_usech, text_in):
    if len(text_in) > 1000000:
        return f'''На вход подана строка {len(text_in)} символов. Ограничене модели - 1 млн. символов.'''
    # usl_cod_model = 'gpt', 'bert', 'rubert' # имя модели
    # dolya_usech = 90 # на сколько ужать (какую долю отсечь)
    global punkt_downloaded
    if not punkt_downloaded:
        nltk.download('punkt')
        nltk.download('punkt_tab')
        punkt_downloaded = True
        
    n = int(len(nltk.sent_tokenize(text_in)))
    size1 = int(round(n*(float(dolya_usech)/100)) + 1)
    size = int((n - size1) + 1)
    if usl_cod_model == 'gpt':
        name_model = 'sberbank-ai/rugpt3small_based_on_gpt2'
    if usl_cod_model == 'bert':
        name_model = 'bert-base-multilingual-cased'
    if usl_cod_model == 'rubert':
        name_model = 'DeepPavlov/rubert-base-cased'
    
    config = AutoConfig.from_pretrained(name_model, output_hidden_states = True)
    custom_tokenizer = AutoTokenizer.from_pretrained(name_model)
    custom_model = AutoModel.from_pretrained(name_model, config=config)
    model = Summarizer(custom_model=custom_model, custom_tokenizer=custom_tokenizer)
    
    result = model(body=text_in, num_sentences=size)
    text_out  = ''.join(result)

    return text_out


def Main():    
    while 1 == 1:
        run_proccess()
        time.sleep(1)


Main()