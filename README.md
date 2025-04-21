# Sistema de Monitoramento e Ranking de Voos

## Descrição

Este sistema monitora dados de partidas de voos do Aeroporto Internacional de Dallas/Fort Worth (DFW), detecta mudanças em horários e portões de embarque, e gera relatórios e rankings de desempenho das companhias aéreas. A aplicação é construída em Python 3.12 e utiliza SQLite como banco de dados local para armazenar snapshots e mudanças detectadas.

## Arquitetura

O sistema é constituído por cinco módulos principais, cada um com responsabilidades específicas:

1. **request.py** - Comunicação com a API de dados de voos
2. **database.py** - Gerenciamento do banco de dados SQLite
3. **monitor.py** - Loop principal de monitoramento e detecção de mudanças
4. **ranking.py** - Análise e ranking de companhias aéreas
5. **reports.py** - Geração de relatórios e visualizações

### Fluxo de Dados

```
API Externa -> request.py -> monitor.py -> database.py -> (ranking.py & reports.py)
```

## Detalhes dos Módulos

### request.py

Este módulo é responsável por comunicar-se com a API Aerodatabox para obter dados atualizados de voos.

**Principais funcionalidades**:
- Autenticação com a API via chaves de API
- Busca dados de partidas do aeroporto DFW
- Validação de dados com modelos Pydantic
- Transformação dos dados brutos em estruturas utilizadas pelo sistema

### database.py

Gerencia todo o acesso ao banco de dados SQLite, incluindo a criação de tabelas, inserção de dados e consultas.

**Tabelas principais**:
- `flight_snapshots`: Armazena cada snapshot de dados de voo coletado
- `flight_changes`: Registra mudanças detectadas entre snapshots

**Funcionalidades principais**:
- Criação de conexões com o banco
- Definição do esquema das tabelas e índices
- Operações CRUD para snapshots e registros de mudanças
- Consultas para recuperação de dados históricos

### monitor.py

Realiza o loop principal de monitoramento, executando consultas regulares à API e detectando mudanças em horários de partida e portões de embarque.

**Principais funcionalidades**:
- Loop de monitoramento com intervalos configuráveis
- Detecção de mudanças usando o método JULIANDAY do SQLite para comparação precisa de timestamps
- Evita duplicação de registros de mudanças iguais
- Salva snapshots e mudanças no banco de dados
- Padronização de todos os timestamps em UTC

**Detecção de mudanças**: O sistema monitora alterações em:
- Horários programados de partida (`scheduled_departure_utc`)
- Horários estimados de partida (`estimated_departure_utc`)
- Portões de embarque (`departure_gate`)

### ranking.py

Analisa os dados coletados para classificar as companhias aéreas com base na frequência e tipo de mudanças detectadas.

**Principais funcionalidades**:
- Cálculo de rankings por mudanças de horário
- Cálculo de rankings por mudanças de portão
- Cálculo de rankings gerais de desempenho
- Relatórios textuais resumidos
- Visualizações usando seaborn/matplotlib:


### reports.py

Gera relatórios detalhados e visualizações para análise dos dados coletados, com foco em voos individuais.

**Principais funcionalidades**:
- Identificação dos voos mais atrasados
- Gráficos informativos mostrando atrasos por voo
- Gráfico consolidado de atrasos dos voos mais afetados
- Gráfico de evolução temporal dos atrasos
- Heatmaps de atrasos por dia da semana e hora do dia
- Histogramas de distribuição dos atrasos significativos (>= 5 minutos de atraso)
- Relatórios textuais detalhados

## Fluxo de Execução

1. **Coleta de Dados**:
   - `python src/monitor.py` inicia o loop de monitoramento
   - Executa consultas à API em intervalos regulares (padrão: 120 segundos)
   - Detecta e registra mudanças no banco de dados

2. **Análise e Relatórios**:
   - Após coletar dados suficientes (recomendado: 24h+):
   - `python src/ranking.py` gera rankings de companhias aéreas
   - `python src/reports.py` cria relatórios e visualizações detalhadas

## Características Técnicas

- **Linguagem**: Python 3.12
- **Banco de Dados**: SQLite (arquivo local `data/flights_monitor_final.db`)
- **Validação de Dados**: Pydantic
- **Logging**: Logfire
- **Visualizações**: Pandas, Matplotlib, Seaborn
- **Requisições HTTP**: http.client nativo

## Configuração

1. Clone o repositório
2. Crie um ambiente virtual: `python -m venv .venv`
3. Ative o ambiente: `.venv\Scripts\activate` (Windows) ou `source .venv/bin/activate` (Unix)
4. Instale as dependências: `pip install pandas numpy matplotlib seaborn pydantic logfire` | **(RECOMENDADO)**: as dependências listadas em `pyproject.toml` podem ser mais facilmente instaladas utilizando o gerenciador `uv`.
5. Crie um arquivo `.env` com suas credenciais:
   ```
   API_KEY=sua_chave_api_aerodatabox
   API_HOST=aerodatabox.p.rapidapi.com
   LOGFIRE_TOKEN=seu_token_logfire
   ```
    - Verifique o arquivo `.env.example` para verificar o formato das variáveis de ambiente.
## Uso

### Monitoramento

```bash
python src/monitor.py
```

Isto iniciará o loop de monitoramento que buscará dados da API em intervalos regulares, detectará mudanças e as armazenará no banco de dados.

### Geração de Rankings

```bash
python src/ranking.py
```

Gera rankings de companhias aéreas com base nas mudanças detectadas.

### Geração de Relatórios

```bash
python src/reports.py
```

Gera relatórios detalhados e visualizações para análise aprofundada.

### Dashboard Interativo

O sistema inclui um dashboard interativo desenvolvido com Streamlit para visualização das análises geradas.

```bash
streamlit run app.py
```

> **Nota:** O dashboard é estático e exibe apenas relatórios pré-gerados. É necessário executar `ranking.py` e `reports.py` antes para gerar os gráficos e relatórios.

### Execução via Docker

O projeto inclui um Dockerfile para facilitar a implantação do dashboard.

```bash
# Construir a imagem Docker
docker build -t flight-monitor-dashboard .

# Executar o container
docker run -p 8501:8501 flight-monitor-dashboard
```

Após a execução, o dashboard estará disponível em `http://localhost:8501`.

> **Importante:** A imagem Docker contém apenas o dashboard Streamlit estático e não inclui a capacidade de atualizar dados em tempo real. A pasta `reports/` deve conter todos os relatórios e gráficos gerados previamente para que o dashboard funcione corretamente.

## Próximos Passos

### Dashboard em Tempo Real

Uma evolução natural do sistema seria integrar o monitor diretamente ao frontend, permitindo a visualização de dados em tempo real e a geração de relatórios sob demanda. Isto poderia ser implementado:

1. Modificando o app Streamlit para acessar diretamente o banco de dados e gerar visualizações dinâmicas
2. Criando um serviço de backend com FastAPI que disponibilizaria endpoints para o dashboard consumir
3. Implementando websockets para atualização em tempo real dos dados no frontend

### Melhorias na Análise de Dados

1. Implementar modelos preditivos para prever atrasos com base em padrões históricos
2. Expandir as análises para incluir outros aeroportos além de DFW
3. Adicionar correlações com dados meteorológicos para análise de causas de atrasos

## Estrutura do Banco de Dados

### Tabela `flight_snapshots`

- `snapshot_id`: ID único do snapshot
- `unique_flight_id`: ID único do voo
- `cycle_timestamp`: Timestamp do ciclo de monitoramento
- `workspace_timestamp`: Timestamp da coleta de dados
- `scheduled_departure_utc`: Horário programado de partida (UTC)
- `estimated_departure_utc`: Horário estimado de partida (UTC)
- `departure_gate`: Portão de embarque
- Outros dados do voo (companhia, número, destino, etc.)

### Tabela `flight_changes`

- `change_id`: ID único da mudança
- `unique_flight_id`: ID único do voo
- `change_detected_cycle_timestamp`: Timestamp do ciclo onde a mudança foi detectada
- `previous_cycle_timestamp`: Timestamp do ciclo anterior à mudança
- `attribute_changed`: Atributo alterado (`scheduled_departure_utc`, `estimated_departure_utc`, `departure_gate`)
- `previous_value`: Valor anterior
- `new_value`: Novo valor
- `change_logged_at`: Timestamp de registro da mudança (UTC)