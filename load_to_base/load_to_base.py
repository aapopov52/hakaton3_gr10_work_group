import psycopg2

conn = psycopg2.connect(
   host="194.87.215.7",
   database="hakaton3",
   user="gen_user",
   password="KMb+9b&J(eUi~5",
   port = '5432'
)

cur = conn.cursor()

id = 0
sql = ''
with open('geo-reviews-dataset-2023.tskv', "r", encoding="utf-8") as f:
    for sline in f:
        id += 1
        if id / 1000 == round(id / 1000):
            print(id)
        #if i >= 100:
        #    break
        if sline[0:len('address=')] != 'address=':
            #print('Ошибка переноса строки:', sline)
            print('Ошибка переноса строки:', sline[1:len('address=')])
            break
        l_address = 1
        l_name_ru = sline.find('name_ru=')
        l_rating = sline.find('rating=')
        l_rubrics = sline.find('rubrics=')
        l_text = sline.find('text=')
        
        #s_address
        if l_name_ru > -1: 
            l2 = l_name_ru
        elif l_rating > -1:
            l2 = l_rating
        elif l_rubrics > -1:
            l2 = l_rubrics
        elif l_text > -1:
            l2 = l_text
        else:
            l2 = len(sline) + 1
        s_address = sline[0:l2-1]
        
        # s_name_ru
        s_name_ru = ''
        if l_name_ru > -1:
            if l_rating > -1:
                l2 = l_rating
            elif l_rubrics > -1:
                l2 = l_rubrics
            elif l_text > -1:
                l2 = l_text
            else:
                l2 = len(sline) + 1
            s_name_ru = sline[l_name_ru:l2-1]
            
        # s_rating
        s_rating = ''
        if l_rating > -1:
            if l_rubrics > -1:
                l2 = l_rubrics
            elif l_text > -1:
                l2 = l_text
            else:
                l2 = len(sline) + 1
            s_rating = sline[l_rating:l2-1]
        
        # s_rubrics
        s_rubrics = ''
        if l_rubrics > -1:
            if l_text > -1:
                l2 = l_text
            else:
                l2 = len(sline) + 1
            s_rubrics = sline[l_rubrics:l2-1]

        # s_text
        s_text = ''
        if l_text > -1:
            l2 = len(sline) + 1
            s_text = sline[l_text:l2 - 1]
            
        if s_address[1:len('address=')]: s_address = s_address[len('address='):len(s_address)]
        if s_name_ru[1:len('name_ru=')]: s_name_ru = s_name_ru[len('name_ru='):len(s_name_ru)]
        if s_rating[1:len('rating=')]: s_rating = s_rating[len('rating='):len(s_rating)]
        if s_rubrics[1:len('rubrics=')]: s_rubrics = s_rubrics[len('rubrics='):len(s_rubrics)]
        if s_text[1:len('text=')]: s_text = s_text[len('text='):len(s_text)]
        
        s_address = s_address.replace("'", "''")
        s_name_ru = s_name_ru.replace("'", "''")
        s_rating = s_rating.replace("'", "''")
        s_rubrics = s_rubrics.replace("'", "''")
        s_text = s_text.replace("'", "''")
        #print (s_address, '\n', s_name_ru, '\n',  s_rating, '\n', s_rubrics, '\n', s_text)
        
        s1 =  f""" 
                    insert into stage_object_otziv(id, address,name_ru,rating,rubrics,text)
                    values ({id}, '{s_address}', '{s_name_ru}', '{s_rating}', '{s_rubrics}', '{s_text}');"""
        
        if len(sql + s1) > 40000:
            cur.execute(sql)
            sql = s1
        else:
            sql += s1
        #print('-------')
if sql != '':
    cur.execute(sql)
conn.commit()

sql = 'select * from public.stage_object_otziv_perv_obr()'
cur.execute(sql)

conn.close()
