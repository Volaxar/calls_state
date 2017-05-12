"""
Утилиты логгирования и конфигурирования

log - интерфейс логгирования, если необходимы расширенные функции, переинициализировать во внешнем модуле
cfg - интерфейс считывания конфигурационного файла
get_options() - функция считывания параметров конфигурации
get_city() - получает список городских для внутренних номеров
"""
import configparser
import logging

log = logging
cfg = None


def _init_config(conf_file):
    """
    Инициализирует экземпляр парсера конфигурации cfg

    :param conf_file: string, имя файла конфигурации
    :return: bool, True - если открыт и прочитан, False - в случае ошибки
    """
    global cfg

    cfg = configparser.ConfigParser()
    try:
        if not cfg.read(conf_file, 'utf-8'):
            log.critical('Конфигурационный файл не загружен')
            return
    except configparser.ParsingError as e:
        log.critical('Ошибка парсинга конфигурационного файла\n%s' % e.message)
        return

    return True


def get_options(section, options=None, ignore_default=False, conf_file='config.ini'):
    """
    Считывает параметры из конфигурационного файла

    Всегда считываются параметры host, user и password

    :param section: string, секция из которой будут считываться параметры
    :param options: [string], параметры для считывания, по умолчанию содержит ['host', 'user', 'password']
    :param ignore_default: bool, игнорировать список опций по умолчанию для options
    :param conf_file: string, имя файла конфигурации
    :return: [string] or string, считанные параметры или None в случае ошибки чтения любого из параметров
    """
    if not cfg:
        if not _init_config(conf_file):
            return

    result_list = []
    options_list = [] if ignore_default else ['host', 'user', 'password']

    if options:
        if not isinstance(options, list):
            options = [options]

        options_list += options

    for option in options_list:
        try:
            result_list.append(cfg.get(section, option))
        except configparser.NoSectionError as e:
            log.error('Секция %s не найдена' % e.section)
            return

        except configparser.NoOptionError as e:
            log.error('Параметр %s в секции %s не найден' % (e.option, e.section))
            return

    if len(result_list) == 1:
        return result_list[0]
    else:
        return result_list


def get_city(num, at_list):
    """
    Получает список городских для внутренних номеров, вн. номеров может быть несколько, разделенных запятой

    :param num: string, список вн. номеров разделенных запятой
    :param at_list: {string: {string}}, {вн_номер: {гор_номер}}
    :return: string, список гор. номеров
    """
    tmp = []

    for t in num.split(','):
        tp = t.strip()

        if tp in at_list:
            tmp += at_list[tp]

    return ', '.join(tmp)
