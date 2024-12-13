from PyQt5.QtWidgets import QApplication, QMainWindow, QTableWidget, QTableWidgetItem, QPushButton, QLabel, QAction, QMenu, QLineEdit, QTextEdit, QComboBox
from PyQt5.QtGui import *
import psycopg2
import time

conn = psycopg2.connect(
   host="194.87.215.7",
   database="hakaton3",
   user="gen_user",
   password="KMb+9b&J(eUi~5",
   port = '5432'
)

class DatabaseTableEditor(QMainWindow):

    
    def __init__(self):
        super().__init__()
        
        self.conn = conn
        self.cur = None
        self.id_object_main = 0
        self.id_zayavka_main_summarise = 0
        self.s_where = ''
        
        self.initUI()
        self.show()
        
        self.loadTable_object_main()
        self.loadTable_otziv()
        self.loadTable_rubrics()
    
    
    def initUI(self):
        #layout = QVBoxLayout()
        #self.setGeometry(100, 100, 1000, 800) 
        
        self.setFixedSize(1500, 1060)
        
        self.cur = self.conn.cursor()
        
        # Главная форма
        self.table_object_main = QTableWidget(self)
        self.table_object_main.setGeometry(10 + 300, 10, 1500 - 300 - 300 - 10 - 10, 500)
        
        
        self.table_object_main.currentCellChanged.connect(self.table_object_main_cell_changed)
        
        # Поля для фильтров
        self.label_filter = QLabel(self)
        self.label_filter.setText ('Фильтры:')
        self.label_filter.setGeometry(10, 10, 290, 20)
        
        self.label_address = QLabel(self)
        self.label_address.setText ('Адрес (like):')
        self.label_address.setGeometry(10, 35, 290, 20)
        self.edit_address = QLineEdit(self)
        self.edit_address.setGeometry(10, 60, 290, 20)
        
        self.label_name_ru = QLabel(self)
        self.label_name_ru.setText ('Наименование (like):')
        self.label_name_ru.setGeometry(10, 85, 290, 20)
        self.edit_name_ru = QLineEdit(self)
        self.edit_name_ru.setGeometry(10, 110, 290, 20)
        
        self.label_rubrica = QLabel(self)
        self.label_rubrica.setText ('Рубрика (like):')
        self.label_rubrica.setGeometry(10, 135, 290, 20)
        self.edit_rubrica = QLineEdit(self)
        self.edit_rubrica.setGeometry(10, 160, 290, 20)
        
        self.label_raiting = QLabel(self)
        self.label_raiting.setText ('Средний рейтинг (от - до включительно):')
        self.label_raiting.setGeometry(10, 185, 290, 20)
        self.edit_raiting_min = QLineEdit(self)
        self.edit_raiting_min.setGeometry(10, 210, 50, 20)
        self.edit_raiting_max = QLineEdit(self)
        self.edit_raiting_max.setGeometry(70, 210, 50, 20)
        
        # так называемый валидатор, чтобы водить целые и дробные числа
        edit_raiting_min_validator = QDoubleValidator(0, 5, 2, self.edit_raiting_min)
        edit_raiting_min_validator.setNotation(QDoubleValidator.StandardNotation)  # стандартная запись
        self.edit_raiting_min.setValidator(edit_raiting_min_validator)
        
        edit_raiting_max_validator = QDoubleValidator(0, 5, 2, self.edit_raiting_max)
        edit_raiting_max_validator.setNotation(QDoubleValidator.StandardNotation)  # стандартная запись
        self.edit_raiting_max.setValidator(edit_raiting_max_validator)
        
        # кнопка - фильтры
        self.btn_filtr = QPushButton(self)
        self.btn_filtr.setText ('Фильтр')
        self.btn_filtr.setGeometry(10, 235, 290, 20)
        self.btn_filtr.clicked.connect(self.loadTable_object_main)
        
        # для суммаризации
        self.label_usl_cod_model = QLabel(self)
        self.label_usl_cod_model.setText ('Модель:')
        self.label_usl_cod_model.setGeometry(1200, 10, 140, 20)
        self.label_dolya_usech = QLabel(self)
        self.label_dolya_usech.setText ('Сжатие (%):')
        self.label_dolya_usech.setGeometry(1350, 10, 140, 20)
        
        self.combo_box_usl_cod_model = QComboBox(self)
        self.combo_box_usl_cod_model.setGeometry(1200, 35, 140, 20)
        self.combo_box_usl_cod_model.addItems(["gpt", "bert", "rubert"])
        
        self.edit_dolya_usech = QLineEdit(self)
        self.edit_dolya_usech.setGeometry(1350, 35, 140, 20)
        self.edit_dolya_usech.setText('50')
        
        edit_dolya_usech_validator = QIntValidator(0, 100, self.edit_dolya_usech)
        self.edit_dolya_usech.setValidator(edit_dolya_usech_validator)
                
        
        self.btn_summarization = QPushButton(self)
        self.btn_summarization.setText ('Суммаризация')
        self.btn_summarization.setGeometry(1200, 60, 290, 20)
        self.btn_summarization.clicked.connect(self.get_summarization_result)
        # вывод результата по сумаризации
        self.edit_summarization_result = QTextEdit(self)
        self.edit_summarization_result.setGeometry(1200, 85, 290, 425)
        
        # Таблицы        
        self.table_rubrics = QTableWidget(self)
        self.table_rubrics.setGeometry(10, 10 + 500 + 10, 290, 500)
        
        self.table_otziv = QTableWidget(self)
        self.table_otziv.setGeometry(310, 10 + 500 + 10, 1500 - 310 - 10, 500)

        
        # Статус bar
        self.status_label = QLabel(self)
        
        self.setWindowTitle('Отзывы')
    
    
    # Обновление основной таблицы
    def loadTable_object_main(self):
        sql = '''select 
                            tm.id,
                            tm.address,
                            tm.name_ru,
                            tm.cnt_otziv,
                            tm.rating_average
                            from object_main tm
                            $WHERE$
                            order by tm.cnt_otziv desc
                            limit 100 '''
        self.loadTable_object_main_where()
        sql = sql.replace('$WHERE$', self.s_where)
        self.cur.execute(sql)
        rows = self.cur.fetchall()
        
        self.table_object_main.setRowCount(len(rows))
        self.table_object_main.setColumnCount(len(rows[0]))
        
        column_labels = ['УН', 'Адрес', 'Наименование', 'кол-во отзывов', ' средний балл']
        self.table_object_main.setHorizontalHeaderLabels(column_labels)
        self.table_object_main.setColumnWidth(0, 50)
        self.table_object_main.setColumnWidth(1, 350)
        self.table_object_main.setColumnWidth(2, 200)
        self.table_object_main.setColumnWidth(3, 100)
        self.table_object_main.setColumnWidth(4, 100)
        
        #print(rows)
        for i, row in enumerate(rows):
            for j, value in enumerate(row):
                item = QTableWidgetItem(str(value))
                if i == 0 and j == 0:
                    self.id_object_main = int(value)
                    self.statusBar().showMessage(str(value))
                self.table_object_main.setItem(i, j, item)
        self.table_object_main.resizeRowsToContents()
        
        if self.table_object_main.rowCount() > 0:
            self.table_object_main.setCurrentCell(0, 0)
            self.loadTable_otziv()
            self.loadTable_rubrics()
            
    
    
    def loadTable_object_main_where(self):
        self.s_where = ''
        if self.edit_address.text() != '':
            if self.s_where != '': self.s_where += ' and \n'
            self.s_where += f'''tm.address like '%{self.edit_address.text().upper()}%' '''


        if self.edit_name_ru.text() != '':
            if self.s_where != '': self.s_where += ' and \n'
            self.s_where += f'''tm.name_ru like '%{self.edit_name_ru.text().upper()}%' '''
            
        if self.edit_rubrica.text() != '':
            if self.s_where != '': self.s_where += ' and \n'
            self.s_where += f'''tm.id  in (select tc.id_object_main
                                            from rubrics tc
                                            left join spr_rubrics tc2 on tc2.id = tc.id_spr_rubrics
                                            where tc2.rubrica like '%{self.edit_rubrica.text().upper()}%') '''
        
        if self.edit_raiting_min.text() != '':
            if self.s_where != '': self.s_where += ' and \n'
            self.s_where += f'''tm.rating_average >= {self.edit_raiting_min.text().replace(',', '.')} '''
        
        if self.edit_raiting_max.text() != '':
            if self.s_where != '': self.s_where += ' and \n'
            self.s_where += f'''tm.rating_average >= {self.edit_raiting_max.text().replace(',', '.')} '''
        
        if self.s_where != '': self.s_where = ' where \n' + self.s_where
    
    
    # Таблица отзывов
    def loadTable_otziv(self):
        if self.id_object_main is None:
            return
        
        sql = f'''select rating, text 
                    from otziv 
                    where id_object_main = {self.id_object_main} 
                    order by rating desc'''
        #print(sql)
        self.cur.execute(sql)
        rows = self.cur.fetchall()
        self.table_otziv.setRowCount(len(rows))
        self.table_otziv.setColumnCount(2)#(len(rows[0]))
        column_labels = ['УН', 'Отзыв']
        self.table_otziv.setHorizontalHeaderLabels(column_labels)
        self.table_otziv.setColumnWidth(0, 50)
        self.table_otziv.setColumnWidth(1, 1050)
        
        for i, row in enumerate(rows):
            for j, value in enumerate(row):
                item = QTableWidgetItem(str(value))
                self.table_otziv.setItem(i, j, item)
    
        self.table_otziv.resizeRowsToContents()
        if self.table_otziv.rowCount() > 0:
            self.table_otziv.setCurrentCell(0, 0)
            
        
        
    # Рубрики
    def loadTable_rubrics(self):
        if self.id_object_main is None:
            return
        
        sql = f'''select rubrica
                    from rubrics
                    left join spr_rubrics on spr_rubrics.id = rubrics.id_spr_rubrics
                    where id_object_main = {self.id_object_main}
                    order by rubrica'''
        #print(sql)
        self.cur.execute(sql)
        rows = self.cur.fetchall()
        
        self.table_rubrics.setRowCount(len(rows))
        self.table_rubrics.setColumnCount(1)#(len(rows[0]))
        column_labels = ['Рубрика']
        self.table_rubrics.setHorizontalHeaderLabels(column_labels)
        self.table_rubrics.setColumnWidth(0, 250)
        
        for i, row in enumerate(rows):
            for j, value in enumerate(row):
                item = QTableWidgetItem(str(value))
                self.table_rubrics.setItem(i, j, item)
        self.table_rubrics.resizeRowsToContents()
        
        if self.table_rubrics.rowCount() > 0:
            self.table_rubrics.setCurrentCell(0, 0)
    
    
    # перемещаемся по ячейкам таблицы object_main
    def table_object_main_cell_changed(self, currentRow, currentColumn, previousRow, previousColumn):
        if currentRow is not None and currentRow != currentColumn:
            self.id_object_main = int(self.table_object_main.item(currentRow, 0).text())
            smess = str(self.id_object_main)
            self.statusBar().showMessage(smess)
            self.loadTable_otziv()
            self.loadTable_rubrics()
    
    
    def get_summarization_result(self):
        if self.id_object_main == 0 : return
        self.edit_summarization_result.setText('запрос направлен ...')
        cur = self.conn.cursor()

        sdolya_usech = self.edit_dolya_usech.text()
        if sdolya_usech == '':
            self.edit_dolya_usech.setText('50')
            sdolya_usech = '50'
        ldolya_usech = int(sdolya_usech)
        
        sql = f''' do $$
                  declare
                      id_zayavka_main_summarise_ int;
                  begin  
                  insert into zayavka_main_summarise(dt_load,                    usl_cod_model,       dolya_usech)
                                               values (now(), '{self.combo_box_usl_cod_model.currentText()}', {str(ldolya_usech)})
                  RETURNING id INTO id_zayavka_main_summarise_;
               
                  RAISE INFO 'id_zayavka_main_summarise_%', id_zayavka_main_summarise_;
                                                
                  end; $$ '''
        
        self.conn.notices = []
        cur.execute(sql)
        conn.commit()
        self.id_table_load_run = 0
        for notice in conn.notices:
            if 'id_zayavka_main_summarise' in notice:
                self.id_zayavka_main_summarise = int(notice[notice.find('id_zayavka_main_summarise') + len('id_zayavka_main_summarise') + 1:])
                break
        
        sql = f''' insert into zayavka_main_summarise_detal (id_zayavka_main_summarise, id_otziv) 
                    select {self.id_zayavka_main_summarise}, id
                    from otziv
                    where id_object_main = {self.id_object_main}
                '''
        cur.execute(sql)
        conn.commit()
        
        # ждём обощённый комментарий от сервера
        while 1 == 1:
            time.sleep(1) 
            sql = f''' select id from zayavka_main_summarise where id = {self.id_zayavka_main_summarise}  and dt_out is not null '''
            cur.execute(sql)
            rows = cur.fetchall()
            if len(rows) > 0:
                if rows[0][0] is not None:
                    sql = f''' select result from zayavka_main_summarise_result where id_zayavka_main_summarise = {self.id_zayavka_main_summarise} '''
                    cur.execute(sql)
                    rows = cur.fetchall()        
                    if rows[0][0] is not None:
                        self.edit_summarization_result.setText(rows[0][0])
                    sql = f''' delete from zayavka_main_summarise where id = {self.id_zayavka_main_summarise} '''
                    cur.execute(sql)
                    
                    break    
        
        self.id_zayavka_main_summarise = 0
        cur.close()
        conn.commit()
        
def main ():
    #app = QApplication(sys.argv)
    #editor = DatabaseTableEditor(conn)
    #sys.exit(app.exec_())
    
    app = QApplication([])
    win = DatabaseTableEditor()
    win.show()
    app.exec()


main()