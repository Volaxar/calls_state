import re
import logging

from collections import defaultdict
from datetime import datetime

import importer
import exporter
import utils

from logger import DiffFileHandler


def get_ext_str(raw_ad, ext):
    ext_str = ext

    if raw_ad[ext]:
        ext_str = '%s (%s)' % (ext_str, ', '.join(raw_ad[ext_str]))

    return ext_str


def main():
    log = logging.getLogger('numlist')
    log.setLevel(logging.INFO)

    formatter = logging.Formatter('[%(asctime)s] %(levelname)-8s %(filename)s[LINE:%(lineno)d]# %(message)s')

    handler = DiffFileHandler()
    handler.setLevel(logging.INFO)
    handler.setFormatter(formatter)
    log.addHandler(handler)

    utils.log = log

    ad_list = importer.get_ad_list()  # Импортируем список сотрудников из AD
    if not ad_list:
        log.critical('Не удалось загрузить список сотрудников из AD')
        exit()

    at_inc_list = importer.get_at_inc_list()  # Импортируем список гор. входящих номеров из БД Астериска
    at_out_list = importer.get_at_out_list()  # Испортируем список гор. исходящих номеров из БД Астериска

    re_exp_date = re.compile(r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}')

    raw_ad = defaultdict(set)

    for x in ad_list:
        if x.displayName:
            exp_date = re_exp_date.match(str(x.accountExpires))

            # Если истекло время действия учётной записи, пропускаем
            if exp_date:
                if datetime.strptime(exp_date.group(), '%Y-%m-%d %H:%M:%S') < datetime.now():
                    continue

            itn = [i.strip() for i in str(x.telephoneNumber).replace('[]', '').split(',')]

            for i in itn:
                raw_ad[i].add(str(x.cn))

    raw = {}

    for lk, lv in [('inc', at_inc_list), ('out', at_out_list)]:
        for k, v in lv.items():
            for i in v:
                if k not in raw:
                    raw[k] = {'inc': [], 'out': []}

                raw[k][lk].append(get_ext_str(raw_ad, i))

    exporter.export_xls(raw)

    # Импортируем звонки из подробного лога Астериска
    full_log = importer.get_full_log(datetime(2017, 1, 1))
    
    exporter.export_xls_brief(full_log)
    exporter.export_xls_full(full_log)


if __name__ == '__main__':
    main()
