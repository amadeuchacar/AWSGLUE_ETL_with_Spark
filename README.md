# Projeto - ETL utilizando AWS Glue e Spark

Projeto realizado no curso "*Data Lake in AWS - Easiest Way to Learn*".

## Objetivo 🎯
Realizar um processo de ETL em um arquivo que possui dados entre 2017 e 2022 de um ranking global de universidades, utilizando o AWS Glue em conjunto com o Spark, com as seguintes etapas:
1. Extrair um arquivo *.csv* de um bucket S3;
2. Realizar a transformação deste arquivo, realizando um pré-processamento (limpeza de dados);
3. Carregar este arquivo transformado em um outro bucket S3;

## Requisitos 📄
1. Bucket S3 com o arquivo *.csv*, presente neste repositório dentro do diretório "csv";
2. Database no AWS Glue, que irá armazenar as tabelas do arquivo *.csv* que será lido;
3. Crawler do AWS Glue para capturar os metadados do arquivo *.csv* lido e construir uma tabela com os dados;
4. Jupyter notebook com o código de ETL presente neste repositório no diretório "*notebooks*";
5. Bucket S3 destino, onde será carregado o arquivo após o processo de ETL;

## Desenvolvimento do código de ETL 👨🏻‍💻

Todas estas etapas estão comentadas no arquivo do notebook presente neste repositório.

### 1️⃣ Primeira etapa - Criação do ambiente
1. Criação do ambiente Spark, início de sessão do Spark e do Glue;
2. Importação de variáveis de ambiente para proteger dados sensíveis (nome do banco de dados criado no Glue, da tabela gerada pelo Crawler, do *Transformation Context* e o path do bucket S3 destino);

### 2️⃣ Segunda etapa - Trabalhando com os dados
1. Importar os dados para o ambiente do notebook, puxando os dados da tabela criada pelo Crawler para uma variável do tipo DynamicFrame;
2. Converter o DynamicFrame para DataFrame para facilitar o trabalho;
3. Realizar os processo de limpeza (transformação dos dados), utilizando Spark SQL;
4. Com os dados transformados, voltar esses dados para uma variável DynamicFrame;

### 3️⃣ Terceira etapa - Carregar os dados
1. Carregar os dados armazenados na variável do tipo DynamicFrame para um arquivo que será armazenado no bucket S3 de destino, conforme caminho do destino especificado;
2. Commit no job do Glue, para que o Glue possa rastrear os dados que já foram processados;
