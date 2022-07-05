# Python Mini Pipeline
Projeto construido para ler um arquivo .csv, fazer a transformação e limpeza dos dados e inserir em um banco de dados.
## Instalação

Instalar depedencias

```bash
  make venv 
  source .venv/bin/activate
  make init

  dev mode
  pip install -r requirements/dev.txt
```

## Iniciando o Docker
```bash
docker-compose up --build
```
## Uso/Exemplos

```bash
python src/main.py --input any_file.csv
```


## Variáveis de Ambiente

Para rodar esse projeto, você vai precisar adicionar as seguintes variáveis de ambiente no seu .env

```
MYSQL_ROOT_PASSWORD=
MYSQL_DATABASE=
```

## Stack utilizada
Python 3.10, Pandas, Python-Dotenv, MySQL Conector, Docker-Compose