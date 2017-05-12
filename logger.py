"""
Модуль содержит класс с расширенными возможностями записи логов в файлы.

DiffFileHandler в отличии от класса FileHandler, может создавать файлы в подкаталогах
("каталог/год/месяц/") и давать им динамические имена основанные на текущей
дате (префик_YYYY-MM-DD.расширение)

"""
import datetime
import os
from logging import StreamHandler, Handler


class DiffFileHandler(StreamHandler):

    def __init__(self, prefix='', ext='log', folder='logs', year=True, month=True, encoding='utf-8'):
        """
        Класс записи логов в файл с расширинным функционалом. Собственных публичных методов не имеет.
        Переопределяет родительские методы close и emit.

        :param prefix: string, префикс в названии файла, отделяется от даты символом "_", по умолчанию пустой
        :param ext: string, расширение файлов логов, по умолчанию "log"
        :param folder: string, каталог верхнего уровня для хранения файлов логов, по умолчанию "logs"
        :param year: bool, добавить к пути файлов логов год, по умолчанию True
        :param month: bool, добавлять к пути файлов логов, по умолчанию True
        :param encoding: string, кодировка файла логов, по умолчанию "utf-8"
        """
        self.filename = ''
        self.prefix = prefix
        self.ext = ext
        self.folder = folder
        self.year = year
        self.month = month
        self.encoding = encoding
        self.stream = None
        
        # Вызывается конструктор прапредка класса, чтобы заранее не открывать файл логов,
        # только перед непосредственной записью строки лога в файл
        Handler.__init__(self)

    def _get_filename(self):
        """
        Создание строки полного пути к файлу лога в зависимости от префикса, расширения, необходимости создать
        подкаталоги и текущей даты.
        
        :return: string, полный путь к файлу лога
        """
        now = datetime.datetime.now()

        folder = []

        if self.folder:
            folder.append(self.folder)

        if self.year:
            folder.append('%04d' % now.year)

        if self.month:
            folder.append('%02d' % now.month)

        folder = os.path.sep.join(folder)

        if not os.path.exists(folder):
            os.makedirs(folder)

        file = []

        if self.prefix:
            file.append('%s_' % self.prefix)

        file.append(now.strftime('%Y-%m-%d'))

        if self.ext:
            file.append('.%s' % self.ext)

        file = ''.join(file)

        return os.path.join(folder, file)

    def _open(self):
        """
        Функция сохраняет полный путь к файлу логов и открывает его.

        :return: TextIOWrapper, дескриптор открытого файла логов
        """
        self.filename = self._get_filename()

        return open(self.filename, mode='a', encoding=self.encoding)

    def close(self):
        """
        Метод закрывает файл логов, если он открыт и закрывает поток.

        """
        self.acquire()
        try:
            try:
                if self.stream:
                    try:
                        self.flush()
                    finally:
                        stream = self.stream
                        self.stream = None
                        if hasattr(stream, "close"):
                            stream.close()
            finally:
                StreamHandler.close(self)
        finally:
            self.release()

    def emit(self, record):
        """
        Метод сохраняет сообщение в потоке, в данном случае в файле лога.

        :param record: LogRecord, сообщение
        """
        if not self.stream:
            self.stream = self._open()

        # Если изменилась дата со времени прошлого открытия файла, переоткрываем его с новым именем
        if self.filename != self._get_filename():
            self.stream.close()
            self.stream = self._open()

        StreamHandler.emit(self, record)
