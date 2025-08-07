# Projeto de Integração de dados

## Requisitos
- PostgreSQL
- Python

## Setup
1. Clone ou instale o repositório

    ```bash
    git clone https://github.com/PetersonNave/data-pipeline-etl-and-elt.git
    cd data-pipeline-etl-and-elt
    ```
2. Inicie e ative um venv Python

    ```bash
    python -m venv venv
    source venv/Scripts/activate
    ```

    >Para desativar o venv é só usar o comando:
    >```bash
    >deactivate
    >```

3. Instale as dependências

    ```bash
    pip install -r requirements.txt
    ```

4. Mude os valores dentro de `/data-pipeline-etl-and-elt/config.json` para os valores do seu PostgreSQL

## ELT
Abaixo está um passo-a-passo de como executar os códigos da Integração ELT
> Considere, inicialmente, o diretório `/data-pipeline-etl-and-elt` como `.`

1. Mude o diretório para `./ELT/covid_elt_project`:

    ```bash
    cd ELT/covid_elt_project
    ```

2. Execute os blocos de `./covid-19-elt.ipynb` seguindo a ordem de cima para baixo
>Faça isso com o python venv ativado

3. Faça um debug e crie/corrija o conteúdos dos .yml que indicarem erro:
    ```bash
    dbt debug
    ```
4. Execute os scripts SQL com o dbt:
    ```bash
    dbt run
    ```
    >Talvez você precise instalar dependências dbt com `dbt deps`

## ETL
Abaixo está um passo-a-passo de como executar os códigos da Integração ETL
> Considere, inicialmente, o diretório `/data-pipeline-etl-and-elt` como `.`

1. Mude o diretório para `./ETL`:

    ```bash
    cd ETL
    ```

2. Execute os blocos de `./covid-19-etl.ipynb` seguindo a ordem de cima para baixo
>Faça isso com o python venv ativado