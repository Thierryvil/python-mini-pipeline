from mysql.connector import connect
from src.infra.database import Database
from dataclasses import dataclass
from logging import Logger


@dataclass()
class MySQLConfig:
    database_name: str
    host: str = 'localhost'
    port: str = 3306
    password: str = 'root'
    username: str = 'root'


class MySQLAdapter(Database):
    def __init__(self, config: MySQLConfig, log: Logger):
        self.config = config
        self.log = log

    def __enter__(self):
        self.log.info(f'[MYSQL] Conectado ao banco de dados: {self.config.username}@{self.config.host}:{self.config.port}')
        self.connection = connect(
            user=self.config.username,
            password=self.config.password,
            host=self.config.host,
            database=self.config.database_name,
            port=self.config.port,
            connect_timeout=5
        )
        self.log.info('[MYSQL] Banco de dados conectado!')
        return self.connection.cursor()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connection.close()
        self.log.info('[MYSQL] Banco de dados desconectado.')
