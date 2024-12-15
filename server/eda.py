
from sklearn.feature_extraction.text import CountVectorizer
import nltk
from nltk.corpus import stopwords
from pymystem3 import Mystem

punkt_downloaded = False  # Глобальный флаг

#original_text = "Приходим на осмотр раз в год,очень нравится офтальмолог Брязгина Анастасия Александровна! Очень внимательная,грамотная,все расскажет подробно,осмотр проводит тщательно! Посмотрит на всех аппаратах,что требуется,даст рекомендации! Девочки очень внимательны,подберут и оправу и линзы. Очень часто бывают акции Оправа в подарок, на данный момент акция вторые солнечные очки в подарок. Отличная Оптика."
#summary_text = "Приходим на осмотр раз в год,очень нравится офтальмолог Брязгина Анастасия Александровна! Очень внимательная,грамотная,все расскажет подробно,осмотр проводит тщательно! Очень часто бывают акции Оправа в подарок, на данный момент акция вторые солнечные очки в подарок."

def text_sopost(original_text, summary_text):
    
    
    m = Mystem()
    original_text = ' '.join(m.lemmatize(original_text)).strip()
    summary_text = ' '.join(m.lemmatize(summary_text)).strip()
    
    global punkt_downloaded
    if not punkt_downloaded:
        nltk.download('stopwords')
        punkt_downloaded = True
    
    stop_words = stopwords.words('russian')
    vectorizer = CountVectorizer(stop_words=stop_words, ngram_range=(1, 1))
    
    # Считаем частоты слов
    original_freq = vectorizer.fit_transform([original_text]).toarray()[0]
    summary_freq = vectorizer.transform([summary_text]).toarray()[0]
    vocab = vectorizer.get_feature_names_out()
    
    #print ('Слова', '\t', 'Оригинал', '\t', 'Суммаризация')
    #for i in range (len(original_freq)):
    #    print (vocab[i], '\t', original_freq[i], '\t', summary_freq[i])
    
    #сортируем по убыванию
    cnt_sort = sorted(set(original_freq), reverse=True)
    
    # выбираем первые 10
    arr_result = []
    for i_num in cnt_sort:
        for i in range (len(original_freq)):
            if original_freq[i] == i_num:
                if len(arr_result) < 10:
                    arr_result.append([vocab[i], original_freq[i], summary_freq[i]])
                else:
                    break
        if len(arr_result) >= 10:
            break
    #print ('------')
    s_out = ''
    cnt = 0
    for row in arr_result:
        if s_out != '': s_out = s_out + '\n'
        s_out = s_out + row[0] + '\t' + str(row[1]) + '\t' + str(row[2])
        if row[2] > 0: cnt += 1
        
    b_out = 0
    if len(arr_result) > 0:
        b_out = cnt / len(arr_result)
    
    return b_out, s_out

#print(b_out, '\n', s_out)  


