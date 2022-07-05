from logging import Logger, NOTSET, FileHandler, Formatter, StreamHandler
from os import mkdir
from os.path import isdir, join
from time import strftime


class Log(Logger):
    def __init__(self, diretorio: str, name: str = 'log'):
        if not isdir(diretorio):
            mkdir(diretorio)
        super().__init__(name, NOTSET)
        file = FileHandler(join(diretorio, f"{strftime('%Y-%m-%d')}.log"))
        formatter = Formatter('%(asctime)s %(message)s', '%Y/%m/%d - %H:%M:%S')
        console = StreamHandler()
        file.setFormatter(formatter)
        console.setFormatter(formatter)
        self.addHandler(file)
        self.addHandler(console)
        self.info(f'{ "-" * 5} PROCESSO INICIADO. {"-" * 5}')
