import time
import psycopg2
import eda # для расчёта метрик EDA

#original_text = "Приходим на осмотр раз в год,очень нравится офтальмолог Брязгина Анастасия Александровна! Очень внимательная,грамотная,все расскажет подробно,осмотр проводит тщательно! Посмотрит на всех аппаратах,что требуется,даст рекомендации! Девочки очень внимательны,подберут и оправу и линзы. Очень часто бывают акции Оправа в подарок, на данный момент акция вторые солнечные очки в подарок. Отличная Оптика."
#summary_text = "Приходим на осмотр раз в год,очень нравится офтальмолог Брязгина Анастасия Александровна! Очень внимательная,грамотная,все расскажет подробно,осмотр проводит тщательно! Очень часто бывают акции Оправа в подарок, на данный момент акция вторые солнечные очки в подарок."

def run_proccess(conn):
    cur = conn.cursor()
    sql = f''' select id, 
                    text_otl_in,  text_neitral_in,  text_bad_in, 
                    text_otl_out, text_neitral_out, text_bad_out
                from zayavka_main_summarise_test
                where b_calc_metrika is null
                limit 1; '''
    cur.execute(sql)
    rows = cur.fetchall()
    # записей нет - выходим
    if len(rows) == 0:
        cur.close()
        conn.close()
        return 0
    id_zayavka_main_summarise_test = 0
    text_otl_in = ''
    text_neitral_in = ''
    text_bad_in = ''
    text_otl_out = ''
    text_neitral_out = ''
    text_bad_out = ''
                    
    id_zayavka_main_summarise = rows[0][0]
    usl_cod_model = rows[0][1]
    dolya_usech = rows[0][2]
    if rows[0][3] is not None:
        id_object_main = rows[0][3]

    id_zayavka_main_summarise_test = rows[0][0]
    if rows[0][1] is not None: text_otl_in = rows[0][1]
    if rows[0][2] is not None: text_neitral_in = rows[0][2]
    if rows[0][3] is not None: text_bad_in = rows[0][3]
    if rows[0][4] is not None: text_otl_out = rows[0][4]
    if rows[0][5] is not None: text_neitral_out = rows[0][5]
    if rows[0][6] is not None: text_bad_out = rows[0][6]

    metrika_otl = -1
    metrika_otl_detal = ''
    metrika_neitral = -1
    metrika_neitral_detal = ''
    metrika_bad = -1
    metrika_bad_detal = ''
    
    
    if text_otl_in != 'отсутствуют' and \
       text_otl_in != 'отсутствуют' and \
       text_otl_out != '':
        metrika_otl, metrika_otl_detal = eda.text_sopost(text_otl_in, text_otl_out)
       
    if text_neitral_in != 'отсутствуют' and \
       text_neitral_in != 'отсутствуют' and \
       text_neitral_out != '':
        metrika_neitral, metrika_neitral_detal = eda.text_sopost(text_neitral_in, text_neitral_out)
    
    if text_bad_in != 'отсутствуют' and \
       text_bad_in != 'отсутствуют' and \
       text_bad_out != '':
        metrika_bad, metrika_bad_detal = eda.text_sopost(text_otl_in, text_otl_out)
    sql = f''' update zayavka_main_summarise_test tu
                set b_calc_metrika = true,
                    metrika_otl = case when {metrika_otl} = -1 then null else {metrika_otl} end,
                    metrika_otl_detal = '{metrika_otl_detal}',
                    metrika_neitral = case when {metrika_neitral} = -1 then null else {metrika_neitral} end,
                    metrika_neitral_detal = '{metrika_neitral_detal}',
                    metrika_bad = case when {metrika_bad} = -1 then null else {metrika_bad} end,
                    metrika_bad_detal = '{metrika_bad_detal}'
                where id = {id_zayavka_main_summarise_test} '''
    cur.execute(sql)
    conn.commit()
    
    return 1


def main():
    conn = psycopg2.connect(
           host="194.87.215.7",
           database="hakaton3",
           user="gen_user",
           password="KMb+9b&J(eUi~5",
           port = '5432'
           )
    while run_proccess(conn) == 1:
        pass
    
    conn.close()
    
    time.sleep(60)
    
main()
#print(b_out, '\n', s_out)  


