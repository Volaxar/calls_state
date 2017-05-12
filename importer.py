import re
from collections import defaultdict
from datetime import datetime

import pymysql
from ldap3 import Server, Connection, ALL, NTLM
from ldap3.core.exceptions import LDAPSocketOpenError, LDAPBindError
from pymysql.err import OperationalError

from utils import log, get_options


re_num = re.compile(r'.*(\d{3})(\d{2})(\d{2})$')


def get_cm(num):
    """
    Форматирование городского номера
    
    :param num: "Сырой" номер 
    :return:  Форматированный номер, формат "XXX-XX-XX"
    """
    num_gr = re_num.match(num)

    if num_gr and len(num_gr.groups()) == 3:
        return '%s-%s-%s' % (num_gr.group(1), num_gr.group(2), num_gr.group(3))

    return ''


def get_ad_list():
    """"
    Импортируем список сотрудников из AD

     Фильтр !(userAccountControl:1.2.840.113556.1.4.803:=2) исключает заблокированные учётные записи

     :return: [{}], cписок словарей с аттрибутами пользователей
    """
    raw = []
    options_list = get_options('ad')
    ad_search = get_options('main', 'ad_search', True)

    if options_list:
        host, user, password = options_list

        server = Server(host, get_info=ALL)

        try:
            with Connection(server, user, password, authentication=NTLM, auto_bind=True) as conn:
                filter_str = '(&(objectclass=person)(!(userAccountControl:1.2.840.113556.1.4.803:=2)))'

                conn.search(ad_search, filter_str, attributes=[
                    'cn',   # Фамилия и инициалы
                    'displayName',  # ФИО
                    'telephoneNumber',  # Внутренние номера
                    'accountExpires'  # Дата блокировки учётной записи
                ])

                raw = conn.entries
        except LDAPSocketOpenError as e:
            log.error(e)
        except LDAPBindError:
            log.error('Ошибка доменной авторизации')

    return raw


def get_at_inc_list():
    """
    Импортируем список городских входящих номеров из БД АТС

    :return: {string: {string}}, {гор_номер: {вн_номер, ...}}
    """
    raw = defaultdict(set)

    options_list = get_options('asterisk', 'db')

    if options_list:
        host, user, password, db = options_list

        try:
            with pymysql.connect(host, user, password, db) as cur:

                re_group = re.compile(r'ext-group,(\d{3}),1')
                re_im = re.compile(r'from-did-direct,(\d{4}),1')
                re_ivr = re.compile(r'ivr-(\d+),s,1')

                # Запрос, привязка групп к вн. номерам
                cur.execute("SELECT grpnum, grplist FROM ringgroups ")

                # Составляем словарь {группа: set(список_вн_номеров)}
                groups_list = dict((k, v.replace('#', '').split('-')) for k, v in cur)

                cur.execute("SELECT ivr_id, selection, dest FROM ivr_dests WHERE ivr_ret = 0")

                ivr_list = defaultdict(dict)

                for ivr_id, sel, dest in cur:
                    ivr_group = re_group.match(dest)

                    if ivr_group:
                        ivr_list[str(ivr_id)][sel] = groups_list[ivr_group.group(1)]
                    else:
                        ivr_im = re_im.match(dest)

                        if ivr_im:
                            ivr_list[str(ivr_id)][sel] = [ivr_im.group(1)]

                # Запрос, привязка гор. номеров к вн. номерам или группам
                cur.execute("SELECT extension, destination FROM incoming "
                            "WHERE LENGTH(extension) = 7 OR LENGTH(extension) = 11")

                for ext, des in cur:

                    # Формат гор. номера "###-##-##"
                    cm = '%s-%s-%s' % (ext[-7:-4], ext[-4:-2], ext[-2:])

                    # Выбираем только группы
                    group = re_group.match(des)

                    if group:
                        for x in groups_list[group.group(1)]:
                            # Добавляем все вн. номера группы связанные с гор. номером
                            raw[cm].add(x)
                    else:
                        # Выбираем только прямые вн. номера
                        im = re_im.match(des)

                        if im:
                            # Добавляем вн. номер связанный с гор. номером
                            # raw[im.group(1)].add(cm)
                            raw[cm].add(im.group(1))
                        else:
                            ivr = re_ivr.match(des)

                            if ivr and ivr.group(1) in ivr_list:
                                for ik, iv in ivr_list[ivr.group(1)].items():
                                    for x in iv:
                                        ivr_cm = '%s/%s' % (cm, ik)

                                        raw[ivr_cm].add(x)

        except OperationalError as e:
            log.error(e)

    return raw


def get_at_out_list():
    """
    Импортируем список исходящих номеров из БД АТС
    
    :return:  {string: {string}}, {гор_номер: {вн_номер, ...}} 
    """
    raw = defaultdict(set)

    options_list = get_options('asterisk', 'db')

    if options_list:
        host, user, password, db = options_list

        try:
            with pymysql.connect(host, user, password, db) as cur:

                cur.execute("SELECT extension, outboundcid FROM users "
                            "WHERE LENGTH(outboundcid) = 11 or LENGTH(outboundcid) = 7")

                for ext, out in cur:
                    raw[get_cm(out)].add(ext)

        except OperationalError as e:
            log.error(e)

    return raw


def get_full_log(p_start, p_end=datetime.now()):
    """
    Парсинг подробного лога Астериска, получение вх. и исх. звонков
    
    :param p_start: Дата начала парсинга
    :param p_end:  Дата окончания парсинга, по умолчанию текущее время
    :return: Словарь звоноков
    """
    raw = defaultdict(dict)

    re_line = re.compile(r'^\[(.*?)\] VERBOSE\[(\d+)\] (\w+\.c): (.+)')
    
    re_out_init = re.compile(r'-- Executing \[\d{5,}@from-internal:1\].*')
    re_out_user = re.compile(r'.*?"AMPUSER=(\d{4})".*')
    re_out_cid = re.compile(r'.*?"USEROUTCID=(\d+)".*')
    re_out_ans = re.compile(r'.*?answered.*')
    re_out_end = re.compile(r'== Spawn.*? exited non-zero.*')

    re_inc_init = re.compile(r'-- Executing \[\d{5,}@from-trunk:1\].*?"__FROM_DID=(\d+)".*')
    re_inc_ans = re.compile(r'.*?answered.*')
    re_inc_call = re.compile(r'-- Called .*?/(\d{4})')
    re_inc_user = re.compile(r'.*?/(\d{4}).*?answered.*')
    re_inc_end = re.compile(r'== Spawn.*? exited non-zero.*')
    # re_inc_xfer = re.compile(r'.*?(\d{4})@from-internal-xfer.*')

    full_path = get_options('main', 'full_path', True)

    with open(full_path) as f:
        for line in f:
            line_match = re_line.match(line)

            if not line_match:
                continue

            raw_mod = line_match.group(3).strip()
            raw_time = datetime.strptime(line_match.group(1).strip(), '%Y-%m-%d %H:%M:%S')
            raw_line = line_match.group(4).strip()

            raw_id = '%s-%s' % (raw_time.date(), line_match.group(2))
            
            if raw_time < p_start or raw_time > p_end:
                continue

            if raw_id not in raw:
                out_init_match = re_out_init.match(raw_line)

                if out_init_match:
                    raw[raw_id]['start'] = raw_time
                    raw[raw_id]['direction'] = 'out'

                    continue

                inc_init_match = re_inc_init.match(raw_line)

                if inc_init_match:
                    raw[raw_id]['start'] = raw_time
                    raw[raw_id]['direction'] = 'inc'
                    raw[raw_id]['cid'] = inc_init_match.group(1)

                continue

            if raw[raw_id]['direction'] == 'out':
                if 'user' not in raw[raw_id]:
                    out_user_match = re_out_user.match(raw_line)

                    if out_user_match:
                        raw[raw_id]['user'] = out_user_match.group(1)

                        continue

                if 'cid' not in raw[raw_id]:
                    out_cid_match = re_out_cid.match(raw_line)

                    if out_cid_match:
                        raw[raw_id]['cid'] = out_cid_match.group(1)

                        continue

                if 'ans' not in raw[raw_id] and raw_mod == 'app_dial.c':
                    out_ans_match = re_out_ans.match(raw_line)

                    if out_ans_match:
                        raw[raw_id]['ans'] = raw_time

                        continue

                if 'end' not in raw[raw_id]:

                    out_end_match = re_out_end.match(raw_line)

                    if out_end_match:
                        raw[raw_id]['end'] = raw_time

                        continue

            if raw[raw_id]['direction'] == 'inc':

                if raw_mod == 'app_dial.c':
                    # if 'xfer' in raw[raw_id]:
                    #     inc_ans_match = re_inc_ans.match(raw_line)
                    #
                    #     if inc_ans_match:
                    #         raw[raw_id]['xfer']['ans'] = raw_time
                    #
                    #         continue

                    inc_user_match = re_inc_user.match(raw_line)

                    if inc_user_match:
                        raw[raw_id]['user'] = inc_user_match.group(1)

                    inc_ans_match = re_inc_ans.match(raw_line)

                    if inc_ans_match:
                        raw[raw_id]['ans'] = raw_time

                        continue

                    if 'user' not in raw[raw_id]:
                        inc_call_match = re_inc_call.match(raw_line)

                        if inc_call_match:
                            raw[raw_id]['call'] = inc_call_match.group(1)

                            continue

                inc_end_match = re_inc_end.match(raw_line)

                if inc_end_match:
                    raw[raw_id]['end'] = raw_time

                    continue
                
                # inc_xfer_match = re_inc_xfer.match(raw_line)
                #
                # if inc_xfer_match:
                #     raw[raw_id]['xfer'] = {
                #         'start': raw_time,
                #         'user': inc_xfer_match.group(1)
                #     }

    result = {}

    for value in raw.values():
        if value['direction'] == 'out':
            if 'cid' in value and 'user' in value:
                cm = get_cm(value['cid'])
                user = value['user']

                duration = 0
                billsec = 0

                if 'start' in value and 'end' in value:
                    duration = (value['end'] - value['start']).seconds

                    if 'ans' in value:
                        billsec = (value['end'] - value['ans']).seconds

                if cm not in result:
                    result[cm] = {'out': {'duration': 0, 'billsec': 0, 'count': 0, 'answer': 0, 'users': {}},
                                  'inc': {'duration': 0, 'billsec': 0, 'count': 0, 'answer': 0, 'users': {}}
                                  }

                result_out = result[cm]['out']

                if user not in result_out['users']:
                    result_out['users'][user] = {'duration': 0, 'billsec': 0, 'count': 0, 'answer': 0}

                result_out['duration'] += duration
                result_out['billsec'] += billsec
                result_out['count'] += 1

                result_out['users'][user]['duration'] += duration
                result_out['users'][user]['billsec'] += billsec
                result_out['users'][user]['count'] += 1

                if billsec:
                    result_out['answer'] += 1
                    result_out['users'][user]['answer'] += 1

        else:
            cm = get_cm(value['cid'])
            user = None
            # xuser = None

            if 'user' in value:
                user = value['user']
            elif 'call' in value:
                user = value['call']

            # if 'xfer' in value:
            #     if 'user' in value['xfer'] and 'ans' in value['xfer']:
            #         xuser = value['xfer']['user']

            duration = 0
            billsec = 0

            if 'start' in value:
                if 'end' in value:
                    duration = (value['end'] - value['start']).seconds

                    if 'ans' in value:
                        billsec = (value['end'] - value['ans']).seconds

            if cm not in result:
                result[cm] = {'out': {'duration': 0, 'billsec': 0, 'count': 0, 'answer': 0, 'users': {}},
                              'inc': {'duration': 0, 'billsec': 0, 'count': 0, 'answer': 0, 'users': {}}
                              }

            result_inc = result[cm]['inc']

            if user and billsec:
                if user not in result_inc['users']:
                    result_inc['users'][user] = {'duration': 0, 'billsec': 0, 'count': 0, 'answer': 0}

                user_duration = duration
                user_billsec = billsec

                # if xuser:
                #     if xuser not in result[cm]['inc']['users']:
                #         result[cm]['inc']['users'][xuser] = {'duration': 0, 'billsec': 0, 'count': 0, 'answer': 0}
                #
                #     user_duration = (value['xfer']['ans'] - value['start']).seconds
                #     user_billsec = (value['xfer']['ans'] - value['ans']).seconds
                #
                #     if duration:
                #         result[cm]['inc']['users'][xuser]['duration'] += (value['end'] - value['xfer']['ans']).seconds
                #         result[cm]['inc']['users'][xuser]['billsec'] += (value['end'] - value['xfer']['ans']).seconds
                #         result[cm]['inc']['users'][xuser]['count'] += 1
                #         result[cm]['inc']['users'][xuser]['answer'] += 1

                result_inc['users'][user]['duration'] += user_duration
                result_inc['users'][user]['billsec'] += user_billsec
                result_inc['users'][user]['count'] += 1
                result_inc['users'][user]['answer'] += 1

            result_inc['duration'] += duration
            result_inc['billsec'] += billsec
            result_inc['count'] += 1

            if billsec:
                result_inc['answer'] += 1

    return result
