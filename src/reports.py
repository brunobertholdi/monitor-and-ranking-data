# --- Imports --- #
import sqlite3
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import os
from typing import Optional
import logfire
from database import get_db_connection
from pydantic import BaseModel

class FlightDelayReport(BaseModel):
    """Modelo Pydantic para relatório de atrasos de voo"""
    flight_id: str
    airline_name: str
    airline_iata: str
    flight_number: str
    destination: str
    total_changes: int
    max_delay_minutes: float
    average_delay_minutes: float
    last_scheduled: str
    last_estimated: str

def extract_delay_data(conn: sqlite3.Connection, min_delay_minutes: float = 5.0) -> pd.DataFrame:
    """
    Extrai dados de mudanças de horário de voos, calculando atrasos.

    Args:
        conn: Conexão com o banco de dados
        min_delay_minutes: Número mínimo de minutos para considerar uma mudança como atraso

    Returns:
        DataFrame com dados de atrasos
    """
    query = """
    SELECT
        f.unique_flight_id,
        f.flight_number,
        f.airline_name,
        f.airline_iata,
        f.destination_name,
        c.attribute_changed,
        c.previous_value,
        c.new_value,
        c.change_detected_cycle_timestamp,
        c.previous_cycle_timestamp,
        c.change_id
    FROM
        flight_changes c
    JOIN
        flight_snapshots f ON c.unique_flight_id = f.unique_flight_id
    WHERE
        c.attribute_changed = 'estimated_departure_utc'
    ORDER BY
        c.unique_flight_id,
        c.change_detected_cycle_timestamp
    """

    df = pd.read_sql_query(query, conn)

    if df.empty:
        return df

    # Convertendo timestamps para datetime e calculando atrasos em minutos
    df['previous_value'] = pd.to_datetime(df['previous_value'])
    df['new_value'] = pd.to_datetime(df['new_value'])
    df['change_detected_cycle_timestamp'] = pd.to_datetime(df['change_detected_cycle_timestamp'])

    # Calcular atraso em minutos (positivo = atraso, negativo = adiantamento)
    df['delay_minutes'] = (df['new_value'] - df['previous_value']).dt.total_seconds() / 60

    # Filtrar apenas mudanças que representam atrasos significativos (> min_delay_minutes)
    # e ignorar completamente adiantamentos (valores negativos)
    significant_delays = df[df['delay_minutes'] >= min_delay_minutes].copy()

    print("=== ANÁLISE DE ATRASOS EM VOOS ===")
    print(f"Regras de negócio aplicadas:\n - Considerando apenas mudanças em 'estimated_departure_utc'\n - Ignorando adiantamentos (valores negativos)\n - Considerando apenas atrasos >= {min_delay_minutes} minutos\n - Contabilizando apenas o maior atraso por voo")
    print("---\n")

    print(f"Total de mudanças de horário estimado detectadas: {len(df)}")
    print(f"→ Adiantamentos (desconsiderados): {len(df[df['delay_minutes'] < 0])}")
    print(f"→ Atrasos menores que {min_delay_minutes} min (desconsiderados): {len(df[(df['delay_minutes'] >= 0) & (df['delay_minutes'] < min_delay_minutes)])}")
    print(f"→ Atrasos significativos (>= {min_delay_minutes} min): {len(significant_delays)}")

    # Agrupar por voo e obter apenas o maior atraso para cada um
    # Isso garante que cada voo seja contado apenas uma vez
    max_delays_per_flight = significant_delays.loc[significant_delays.groupby('unique_flight_id')['delay_minutes'].idxmax()]

    print(f"\n* Total de voos com atrasos significativos: {len(max_delays_per_flight)} *")
    print("===\n")

    return max_delays_per_flight

def identify_most_delayed_flights(df: pd.DataFrame, top_n: int = 10) -> pd.DataFrame:
    """
    Identifica os voos com maiores atrasos.

    Args:
        df: DataFrame com dados de atrasos
        top_n: Número de voos a retornar

    Returns:
        DataFrame com os voos mais atrasados
    """
    # Agrupar por voo e calcular estatísticas
    flight_stats = df.groupby(['unique_flight_id', 'flight_number', 'airline_name', 'airline_iata', 'destination_name']).agg({
        'delay_minutes': ['sum', 'mean', 'max', 'count'],
        'new_value': ['max']  # último horário estimado
    }).reset_index()

    # Renomear colunas
    flight_stats.columns = ['unique_flight_id', 'flight_number', 'airline_name', 'airline_iata', 'destination', 
                           'total_delay_minutes', 'avg_delay_minutes', 
                           'max_delay_minutes', 'change_count', 'last_estimated']

    # Obter o último horário programado para cada voo
    latest_snapshot = df.sort_values('change_detected_cycle_timestamp').groupby('unique_flight_id').last()
    flight_stats = flight_stats.merge(
        latest_snapshot[['previous_value']],
        left_on='unique_flight_id',
        right_index=True
    )
    flight_stats.rename(columns={'previous_value': 'last_scheduled'}, inplace=True)

    # Ordenar pelo atraso máximo
    most_delayed = flight_stats.sort_values('max_delay_minutes', ascending=False).head(top_n)

    return most_delayed

def plot_delay_timeline(df: pd.DataFrame, flight_id: str, output_dir: str = 'reports'):
    """
    Cria um gráfico de barras informativo mostrando os detalhes do atraso para o voo selecionado.
    Em vez de mostrar uma linha temporal que seria inadequada para dados com um único ponto,
    exibe informações relevantes sobre o atraso.

    Args:
        df: DataFrame com dados de atrasos
        flight_id: ID único do voo para plotar
        output_dir: Diretório para salvar o gráfico
    """
    flight_data = df[df['unique_flight_id'] == flight_id].copy()

    if flight_data.empty:
        logfire.warning(f"Sem dados para o voo {flight_id}")
        return

    # Informações do voo
    flight_info = flight_data.iloc[0]
    flight_name = f"{flight_info['airline_iata']} {flight_info['flight_number']} para {flight_info['destination_name']}"

    # Obter informações de atraso
    delay_minutes = flight_info['delay_minutes']
    original_time = flight_info['previous_value']
    new_time = flight_info['new_value']
    detected_time = flight_info['change_detected_cycle_timestamp']

    # Criar gráfico de barras informativo
    plt.figure(figsize=(10, 6))

    # Definir informações para exibir
    labels = ['Atraso (minutos)']
    values = [delay_minutes]

    # Criar barra principal para o atraso
    bars = plt.bar(labels, values, color='#ff7f0e', width=0.4)

    # Adicionar rótulo de valor no topo da barra
    for bar in bars:
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2., height + 3, f'{height:.1f}',
                ha='center', va='bottom', fontweight='bold', fontsize=12)

    # Adicionar título e labels
    plt.title(f"Atraso Detectado - {flight_name}", fontsize=14, pad=20)
    plt.ylim(0, max(120, delay_minutes * 1.2))  # Limite do eixo Y com espaço para texto
    plt.ylabel("Minutos", fontsize=12)

    # Adicionar informações detalhadas como texto no gráfico
    info_text = f"\nHorário Original: {original_time.strftime('%H:%M')}\n"
    info_text += f"Novo Horário: {new_time.strftime('%H:%M')}\n"
    info_text += f"Atraso: {delay_minutes:.1f} minutos\n"
    info_text += f"Detectado em: {detected_time.strftime('%d/%m/%Y %H:%M')}"

    # Colocar o texto na parte inferior do gráfico
    plt.figtext(0.5, 0.01, info_text, ha='center', fontsize=12,
                bbox=dict(facecolor='#f8f9fa', edgecolor='#dee2e6', boxstyle='round,pad=1'))

    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.tight_layout(rect=[0, 0.15, 1, 0.95])

    os.makedirs(output_dir, exist_ok=True)
    safe_id = flight_id.replace('/', '_').replace('\\', '_')
    plt.savefig(f"{output_dir}/delay_timeline_{safe_id}.png")
    plt.close()

    logfire.info(f"Gráfico informativo criado para o voo {flight_id}")

def plot_delay_heatmap(df: pd.DataFrame, output_dir: str = 'reports'):
    """
    Cria um heatmap mostrando a distribuição de atrasos por hora do dia e dia da semana.

    Args:
        df: DataFrame com dados de atrasos
        output_dir: Diretório para salvar o gráfico
    """
    # Extrair hora do dia e dia da semana
    time_data = df.copy()
    time_data['hour'] = time_data['change_detected_cycle_timestamp'].dt.hour
    time_data['day_of_week'] = time_data['change_detected_cycle_timestamp'].dt.day_name()

    # Calcular média de atraso por hora e dia
    heatmap_data = time_data.groupby(['day_of_week', 'hour'])['delay_minutes'].mean().reset_index()

    # Pivotar dados para formato adequado para heatmap
    heatmap_pivot = heatmap_data.pivot(index='day_of_week', columns='hour', values='delay_minutes')

    # Ordem dos dias da semana
    day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    heatmap_pivot = heatmap_pivot.reindex(day_order)

    # Criar gráfico
    plt.figure(figsize=(15, 8))
    sns.heatmap(heatmap_pivot, cmap='coolwarm', center=0,
                annot=True, fmt=".1f", cbar_kws={'label': 'Atraso Médio (minutos)'})

    plt.title('Atrasos Médios por Hora do Dia e Dia da Semana')
    plt.xlabel('Hora do Dia')
    plt.ylabel('Dia da Semana')

    # Salvar gráfico
    os.makedirs(output_dir, exist_ok=True)
    plt.savefig(f"{output_dir}/delay_heatmap.png")
    plt.close()

    logfire.info("Heatmap de atrasos criado")

def plot_airline_delay_comparison(df: pd.DataFrame, output_dir: str = 'reports'):
    """
    Cria um gráfico de barras comparando atrasos médios por companhia aérea.

    Args:
        df: DataFrame com dados de atrasos
        output_dir: Diretório para salvar o gráfico
    """
    # Agrupar por companhia aérea
    airline_delays = df.groupby(['airline_iata', 'airline_name'])['delay_minutes'].agg(['mean', 'max', 'count']).reset_index()

    # Filtrar companhias com número mínimo de mudanças
    min_changes = 5
    airline_delays = airline_delays[airline_delays['count'] >= min_changes]

    # Ordenar por atraso médio
    airline_delays = airline_delays.sort_values('mean', ascending=False)

    if airline_delays.empty:
        logfire.warning("Dados insuficientes para comparação entre companhias aéreas")
        return

    # Criar gráfico
    plt.figure(figsize=(14, 8))

    # Gráfico de barras para atraso médio
    ax = sns.barplot(x='airline_iata', y='mean', data=airline_delays)

    # Adicionar rótulos com nomes das companhias
    for i, row in enumerate(airline_delays.itertuples()):
        ax.text(i, 0.5, row.airline_name, ha='center', rotation=90, color='white', fontweight='bold')

    # Formatar gráfico
    plt.title('Atraso Médio por Companhia Aérea (minutos)')
    plt.xlabel('Código IATA da Companhia')
    plt.ylabel('Atraso Médio (minutos)')
    plt.grid(True, linestyle='--', alpha=0.3, axis='y')

    # Salvar gráfico
    os.makedirs(output_dir, exist_ok=True)
    plt.savefig(f"{output_dir}/airline_delay_comparison.png")
    plt.close()

    logfire.info("Gráfico de comparação de atrasos por companhia aérea criado")

def plot_delay_histogram(df: pd.DataFrame, output_dir: str = 'reports'):
    """
    Cria um histograma mostrando a distribuição dos atrasos.
    Considera apenas atrasos positivos (nenhum adiantamento).

    Args:
        df: DataFrame com dados de atrasos
        output_dir: Diretório para salvar o gráfico
    """
    plt.figure(figsize=(12, 6))

    # Filtrar apenas atrasos positivos e limitar valores extremos para melhor visualização
    filtered_delays = df[df['delay_minutes'] > 0]['delay_minutes']
    max_delay = min(filtered_delays.max(), 120)  # Limitar a 2 horas para melhor visualização
    
    if filtered_delays.empty:
        logfire.warning("Sem dados de atrasos para gerar histograma")
        return

    # Criar histograma
    ax = sns.histplot(filtered_delays, kde=True, bins=30)
    
    # Ajustar eixo X para melhorar visualização
    # Deslocar levemente o eixo para a direita para que o 0 não fique no início do gráfico
    x_min = max(0, filtered_delays.min() * 0.8)  # Um pouco antes do valor mínimo
    plt.xlim(x_min, max_delay)
    
    # Destacar o atraso médio com uma linha vertical
    mean_delay = filtered_delays.mean()
    plt.axvline(x=mean_delay, color='red', linestyle='--', alpha=0.7, label=f'Atraso Médio: {mean_delay:.1f} min')
    plt.legend()

    # Formatar gráfico
    plt.title('Distribuição de Atrasos (-2h a +2h)')
    plt.xlabel('Atraso (minutos)')
    plt.ylabel('Frequência')
    plt.grid(True, linestyle='--', alpha=0.3)

    # Salvar gráfico
    os.makedirs(output_dir, exist_ok=True)
    plt.savefig(f"{output_dir}/delay_histogram.png")
    plt.close()

    logfire.info("Histograma de atrasos criado")

def generate_text_report(most_delayed: pd.DataFrame, output_dir: str = 'reports'):
    """
    Gera um relatório textual com informações sobre os voos mais atrasados.

    Args:
        most_delayed: DataFrame com os voos mais atrasados
        output_dir: Diretório para salvar o relatório
    """
    report = "RELATÓRIO DE VOOS MAIS ATRASADOS\n"
    report += "=" * 50 + "\n\n"

    for i, row in enumerate(most_delayed.itertuples(), 1):
        report += f"{i}. Voo: {row.airline_iata} {row.flight_number} para {row.destination}\n"
        report += f"   Companhia: {row.airline_name}\n"
        report += f"   Atraso Máximo: {row.max_delay_minutes:.1f} minutos\n"
        report += f"   Atraso Médio: {row.avg_delay_minutes:.1f} minutos\n"
        report += f"   Total de Mudanças: {row.change_count}\n"
        report += f"   Último Horário Programado: {row.last_scheduled}\n"
        report += f"   Último Horário Estimado: {row.last_estimated}\n\n"

    # Salvar relatório
    os.makedirs(output_dir, exist_ok=True)
    with open(f"{output_dir}/most_delayed_flights.txt", 'w') as f:
        f.write(report)

    logfire.info("Relatório textual de voos mais atrasados criado")

def plot_consolidated_delays(df: pd.DataFrame, most_delayed: pd.DataFrame, top_n: int = 10, output_dir: str = 'reports'):
    """
    Cria um gráfico consolidado mostrando os atrasos dos voos mais atrasados em um único gráfico.
    Cada voo é representado por uma barra com cor diferente e legenda.
    
    Args:
        df: DataFrame com dados de atrasos
        most_delayed: DataFrame com os voos mais atrasados
        top_n: Número de voos a incluir no gráfico
        output_dir: Diretório para salvar o gráfico
    """
    # Limitar ao número de voos solicitado
    top_flights = most_delayed.head(top_n).copy()
    
    if top_flights.empty:
        logfire.warning("Sem dados suficientes para gerar gráfico consolidado")
        return
    
    # Preparar dados para o gráfico
    plt.figure(figsize=(14, 8))
    
    # Criar labels para o eixo X (códigos dos voos)
    labels = [f"{row.airline_iata} {row.flight_number}" for row in top_flights.itertuples()]
    
    # Valores dos atrasos
    values = top_flights['max_delay_minutes'].values
    
    # Criar barras horizontais coloridas
    colors = plt.cm.tab10.colors[:len(top_flights)]  # Pega cores do mapa de cores tab10
    bars = plt.barh(labels, values, color=colors, height=0.6)
    
    # Adicionar rótulos com os valores dos atrasos
    for i, bar in enumerate(bars):
        width = bar.get_width()
        flight = top_flights.iloc[i]
        plt.text(width + 5, bar.get_y() + bar.get_height()/2, 
                f"{width:.0f} min ({flight.destination})",
                ha='left', va='center', fontsize=9)
    
    # Adicionar informações de data/hora originais e novas como anotações
    # Posicionar as caixas de horários ao lado dos nomes dos voos, não sobre eles
    for i, flight in enumerate(top_flights.itertuples()):
        orig_time = pd.to_datetime(flight.last_scheduled).strftime('%H:%M')
        new_time = pd.to_datetime(flight.last_estimated).strftime('%H:%M')
        plt.text(10, i, f"{orig_time} → {new_time}", ha='left', va='center', fontsize=9,
                bbox=dict(boxstyle='round,pad=0.3', fc='#f8f9fa', ec='#dee2e6', alpha=0.8), zorder=5)
    
    # Formatar o gráfico
    plt.title("Top 10 Voos com Maiores Atrasos", fontsize=16, pad=20)
    plt.xlabel("Atraso (minutos)", fontsize=12)
    plt.grid(axis='x', linestyle='--', alpha=0.7)
    plt.xlim(-45, max(values) * 1.2)  # Ampliar um pouco o eixo X para as anotações
    
    # Adicionar uma legenda com os destinos no canto superior direito
    destination_patches = [plt.Rectangle((0,0), 1, 1, fc=color) 
                          for color in colors[:len(top_flights)]]
    plt.legend(destination_patches, 
              [f"{row.destination}" for row in top_flights.itertuples()],
              loc='upper right', title="Destinos", fontsize=8, framealpha=0.9)
    
    # Ajustar layout e margens
    plt.tight_layout()
    
    # Salvar gráfico
    os.makedirs(output_dir, exist_ok=True)
    plt.savefig(f"{output_dir}/consolidated_delays.png")
    plt.close()
    
    logfire.info("Gráfico consolidado de atrasos criado")

def plot_delay_evolution(conn: sqlite3.Connection, most_delayed: pd.DataFrame, top_n: int = 10, output_dir: str = 'reports'):
    """
    Cria um gráfico de evolução dos atrasos para os voos mais atrasados, mostrando como
    o atraso evolui ao longo do tempo para cada voo, considerando todas as alterações de horário.
    
    Args:
        conn: Conexão com o banco de dados
        most_delayed: DataFrame com os voos mais atrasados
        top_n: Número de voos a incluir no gráfico
        output_dir: Diretório para salvar o gráfico
    """
    # Selecionar os top N voos mais atrasados
    top_flights = most_delayed.head(top_n)
    
    if top_flights.empty:
        logfire.warning("Sem dados suficientes para gerar gráfico de evolução de atrasos")
        return
    
    # Obter IDs dos voos mais atrasados
    flight_ids = top_flights['unique_flight_id'].tolist()
    
    # Consulta para obter todas as mudanças de horário para esses voos
    placeholders = ','.join(['?'] * len(flight_ids))
    query = f"""
    SELECT
        fc.unique_flight_id,
        fs.airline_iata,
        fs.flight_number,
        fs.destination_name,
        fc.attribute_changed,
        fc.previous_value,
        fc.new_value,
        fc.change_detected_cycle_timestamp
    FROM
        flight_changes fc
    JOIN
        flight_snapshots fs ON fc.unique_flight_id = fs.unique_flight_id
    WHERE
        fc.unique_flight_id IN ({placeholders})
        AND fc.attribute_changed = 'estimated_departure_utc'
    ORDER BY
        fc.unique_flight_id,
        fc.change_detected_cycle_timestamp
    """
    
    # Executar consulta
    evolution_data = pd.read_sql_query(query, conn, params=flight_ids)
    
    if evolution_data.empty:
        logfire.warning("Sem dados de evolução de atrasos para os voos selecionados")
        return
    
    # Converter timestamps para datetime
    evolution_data['previous_value'] = pd.to_datetime(evolution_data['previous_value'])
    evolution_data['new_value'] = pd.to_datetime(evolution_data['new_value'])
    evolution_data['change_detected_cycle_timestamp'] = pd.to_datetime(evolution_data['change_detected_cycle_timestamp'])
    
    # Calcular atraso em minutos (positivo = atraso, negativo = adiantamento)
    evolution_data['delay_minutes'] = (evolution_data['new_value'] - evolution_data['previous_value']).dt.total_seconds() / 60
    
    # Ignorar adiantamentos (valores negativos)
    evolution_data = evolution_data[evolution_data['delay_minutes'] >= 0]
    
    # Remover mudanças duplicadas (mesmos valores em timestamps muito próximos)
    # Agrupar mudanças com mesmo atraso em janelas de 5 minutos
    evolution_data['timestamp_rounded'] = evolution_data['change_detected_cycle_timestamp'].dt.floor('5min')
    evolution_data = evolution_data.drop_duplicates(subset=['unique_flight_id', 'delay_minutes', 'timestamp_rounded'])
    
    # Criar um identificador mais legível para o voo
    evolution_data['flight_code'] = evolution_data['airline_iata'] + ' ' + evolution_data['flight_number']
    
    # Preparar o gráfico
    plt.figure(figsize=(14, 8))
    
    # Criar um mapa de cores para os voos
    unique_flights = evolution_data['flight_code'].unique()
    colors = plt.cm.tab10.colors[:len(unique_flights)]
    flight_colors = dict(zip(unique_flights, colors))
    
    # Para armazenar dados da última mudança de cada voo para a legenda
    last_points = {}
    
    # Plotar linhas para cada voo
    for flight_code in unique_flights:
        flight_data = evolution_data[evolution_data['flight_code'] == flight_code].sort_values('change_detected_cycle_timestamp')
        
        # Se temos apenas um ponto, adicionar um ponto no início com atraso zero para mostrar a evolução
        if len(flight_data) == 1:
            # Criar um ponto 10 minutos antes com atraso zero
            row = flight_data.iloc[0].copy()
            row['change_detected_cycle_timestamp'] = row['change_detected_cycle_timestamp'] - pd.Timedelta(minutes=10)
            row['delay_minutes'] = 0
            flight_data = pd.concat([pd.DataFrame([row]), flight_data])
        
        # Plotar a linha com pontos bem marcados
        plt.plot(flight_data['change_detected_cycle_timestamp'], flight_data['delay_minutes'], '-', 
                color=flight_colors[flight_code], label=f"{flight_code} para {flight_data.iloc[0]['destination_name']}",
                linewidth=2)
        
        # Adicionar pontos destacados em cada mudanu00e7a
        plt.scatter(flight_data['change_detected_cycle_timestamp'], flight_data['delay_minutes'], 
                 color=flight_colors[flight_code], s=80, zorder=5, 
                 edgecolor='white', linewidth=1.5)
        
        # Guardar o último ponto para anotar no gráfico
        last_point = flight_data.iloc[-1]
        last_points[flight_code] = (last_point['change_detected_cycle_timestamp'], last_point['delay_minutes'])
    
    # Anotar o valor final de cada linha
    for flight_code, (x, y) in last_points.items():
        plt.annotate(f"{y:.0f} min", xy=(x, y), xytext=(5, 0), textcoords='offset points',
                   fontsize=9, fontweight='bold', color=flight_colors[flight_code])
    
    # Configurar o gráfico
    plt.title("Evolução de Atrasos para os 10 Voos Mais Atrasados", fontsize=16)
    plt.xlabel("Data/Hora da Detecção da Mudança", fontsize=12)
    plt.ylabel("Atraso (minutos)", fontsize=12)
    plt.grid(True, linestyle='--', alpha=0.7)
    
    # Formatar eixo x para mostrar data/hora
    plt.gcf().autofmt_xdate()
    
    plt.legend(fontsize=9, loc='upper left', bbox_to_anchor=(1.1, 1), title="Voos")
    
    # Ajustar layout para acomodar a legenda com mais espaço à direita
    plt.tight_layout(rect=[0, 0, 0.82, 1])
    
    # Salvar gráfico
    os.makedirs(output_dir, exist_ok=True)
    plt.savefig(f"{output_dir}/delay_evolution.png", dpi=120)
    plt.close()
    
    logfire.info("Gráfico de evolução de atrasos criado")
    
def run_reports(db_path: Optional[str] = None, output_dir: str = 'reports'):
    """
    Executa a geração completa de relatórios.

    Args:
        db_path: Caminho para o banco de dados (opcional)
        output_dir: Diretório para salvar os relatórios
    """
    conn = get_db_connection() if db_path is None else sqlite3.connect(db_path)

    try:
        logfire.info("Iniciando geração de relatórios de voos...")

        # Extrair dados
        df = extract_delay_data(conn)

        if df.empty:
            logfire.warning("Não há dados suficientes para gerar relatórios.")
            return

        # Identificar voos mais atrasados
        most_delayed = identify_most_delayed_flights(df)

        # Gerar relatório textual
        generate_text_report(most_delayed)

        # Gerar gráfico consolidado com todos os voos mais atrasados
        plot_consolidated_delays(df, most_delayed, top_n=10)

        # Gerar gráficos individuais para os 5 voos mais atrasados
        for flight_id in most_delayed['unique_flight_id'].head(5):
            plot_delay_timeline(df, flight_id)

        # Gerar heatmap de atrasos
        plot_delay_heatmap(df)

        # Gerar comparação entre companhias
        plot_airline_delay_comparison(df)

        # Gerar histograma de atrasos
        plot_delay_histogram(df)
        
        # Gerar gráfico de evolução de atrasos para os 10 voos mais atrasados
        plot_delay_evolution(conn, most_delayed, top_n=10, output_dir=output_dir)

        # Salvar dados em CSV para análises adicionais
        most_delayed.to_csv(f"{output_dir}/most_delayed_flights.csv", index=False)

        logfire.info(f"Relatórios gerados com sucesso e salvos em '{output_dir}'")

    finally:
        conn.close()

if __name__ == "__main__":
    import os
    os.makedirs('reports', exist_ok=True)
    run_reports()