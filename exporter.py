import xlwt

# import style as ts
from utils import log, get_options


def export_xls(raw):
    """
    Выгрузка структуры телефонной книги в excel

    :param raw: {string: {string: [[string]]}}, структура тел. книги {организация: {отдел: [[данные_сотрудника]]}}
    :return: bool, True - если тел. книга выгружена, False - если произошла ошибка выгрузки
    """

    wb = xlwt.Workbook()
    ws = wb.add_sheet('Список номеров')

    # Заголовок
    ws.write(0, 1, 'Гор. номер')
    ws.write(0, 2, 'Вх.')
    ws.write(0, 3, 'Исх.')

    path = get_options('main', 'xls_path', True)

    if not path:
        log.critical('Ошибка чтения конфигурационного файла, см. ошибки выше')
        return

    line = 1

    for kc in sorted(raw):

        ws.write(line, 1, kc)

        ws.write(line, 2, ', '.join(raw[kc]['inc']))
        ws.write(line, 3, ', '.join(raw[kc]['out']))

        line += 1

    try:
        wb.save(path)
    except PermissionError as e:
        log.error('Недостаточно прав для сохранения файла: %s' % e.filename)
        return
    except FileNotFoundError as e:
        log.error('Неверный путь или имя файла: %s' % e.filename)
        return

    return True


def format_time(seconds):
    """
    Преобразование секунд в строку формата "ЧЧЧЧ:ММ:СС"
    
    :param seconds: int секунды
    :return: str формата "ЧЧЧЧ:ММ:СС"
    """
    s = 0
    m = 0
    h = 0
    
    if seconds > 60:
        m = seconds // 60
    else:
        s = seconds
        
    if m > 60:
        h = m // 60
    
    if m:
        s = seconds - m * 60
    
    if h:
        m = m - h * 60
    
    return '%s:%02d:%02d' % (h, m, s)
    
    
def export_xls_brief(raw):
    """
    Выгрузка краткой (без внутренних номеров) статистики звонков
    
    :param raw: 
    :return: 
    """
    wb = xlwt.Workbook()
    ws = wb.add_sheet('Краткий список')

    # Заголовок
    ws.write(0, 0, 'Гор. номер')
    ws.write_merge(0, 0, 1, 4, 'Вх.')
    ws.write_merge(0, 0, 5, 8, 'Исх.')

    path = get_options('main', 'xls_path_brief', True)

    if not path:
        log.critical('Ошибка чтения конфигурационного файла, см. ошибки выше')
        return

    line = 1
    
    for kc in sorted(raw):
        ws.write(line, 0, kc)
        
        ws.write(line, 1, format_time(raw[kc]['inc']['duration']))
        ws.write(line, 2, raw[kc]['inc']['count'])
        ws.write(line, 3, format_time(raw[kc]['inc']['billsec']))
        ws.write(line, 4, raw[kc]['inc']['answer'])
        
        ws.write(line, 5, format_time(raw[kc]['out']['duration']))
        ws.write(line, 6, raw[kc]['out']['count'])
        ws.write(line, 7, format_time(raw[kc]['out']['billsec']))
        ws.write(line, 8, raw[kc]['out']['answer'])

        line += 1
        
    try:
        wb.save(path)
    except PermissionError as e:
        log.error('Недостаточно прав для сохранения файла: %s' % e.filename)
        return
    except FileNotFoundError as e:
        log.error('Неверный путь или имя файла: %s' % e.filename)
        return

    return True


def export_xls_full(raw):
    """
    Выгрузка полной (с внутренними номерами) статистики звонков
    :param raw: 
    :return: 
    """
    wb = xlwt.Workbook()
    ws = wb.add_sheet('Подробный список')
    
    # Заголовок
    ws.write(0, 0, 'Гор. номер')
    ws.write_merge(0, 0, 1, 5, 'Вх.')
    ws.write_merge(0, 0, 6, 10, 'Исх.')
    
    path = get_options('main', 'xls_path_full', True)
    
    if not path:
        log.critical('Ошибка чтения конфигурационного файла, см. ошибки выше')
        return
    
    line = 1
    
    for kc in sorted(raw):
        ws.write(line, 0, kc)
        ki = raw[kc]['inc']
        ko = raw[kc]['out']
        
        kiu = ki['users']
        kou = ko['users']
        
        ws.write(line, 2, format_time(ki['duration']))
        ws.write(line, 3, ki['count'])
        ws.write(line, 4, format_time(ki['billsec']))
        ws.write(line, 5, ki['answer'])

        ws.write(line, 7, format_time(ko['duration']))
        ws.write(line, 8, ko['count'])
        ws.write(line, 9, format_time(ko['billsec']))
        ws.write(line, 10, ko['answer'])
        
        inc_line = 0
        for inc in sorted(kiu):
            inc_line += 1
            ws.write(line + inc_line, 1, inc)
            ws.write(line + inc_line, 4, format_time(kiu[inc]['billsec']))
            ws.write(line + inc_line, 5, kiu[inc]['answer'])

        out_line = 0
        for out in sorted(kou):
            out_line += 1
            ws.write(line + out_line, 6, out)
            ws.write(line + out_line, 7, format_time(kou[out]['duration']))
            ws.write(line + out_line, 8, kou[out]['count'])
            ws.write(line + out_line, 9, format_time(kou[out]['billsec']))
            ws.write(line + out_line, 10, kou[out]['answer'])

        line += max([inc_line, out_line]) + 1

    try:
        wb.save(path)
    except PermissionError as e:
        log.error('Недостаточно прав для сохранения файла: %s' % e.filename)
        return
    except FileNotFoundError as e:
        log.error('Неверный путь или имя файла: %s' % e.filename)
        return
    
    return True
