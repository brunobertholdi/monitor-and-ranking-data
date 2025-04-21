# Use a imagem base Python 3.12
FROM python:3.12-slim

# Define variáveis de ambiente
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Define o diretório de trabalho
WORKDIR /app

# Copia os arquivos de dependências
COPY pyproject.toml /app/

# Instala as dependências necessárias
RUN pip install --no-cache-dir streamlit pandas

# Copia apenas os arquivos necessários para o dashboard
COPY app.py /app/
COPY reports/ /app/reports/

# Configura a porta para o Streamlit
EXPOSE 8501

# Configura o Streamlit para aceitar conexões externas
ENV STREAMLIT_SERVER_PORT=8501
ENV STREAMLIT_SERVER_ADDRESS=0.0.0.0

# Comando para executar o Streamlit
CMD ["streamlit", "run", "app.py", "--server.address=0.0.0.0", "--server.port=8501"]
