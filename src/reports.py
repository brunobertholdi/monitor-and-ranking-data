# reports.py
import sqlite3
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime, timedelta
import os
from typing import Dict, List, Tuple, Optional
import logfire
from database import get_db_connection
from pydantic import BaseModel, Field

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

def extract_delay_data(conn: sqlite3.Connection, min_changes: int = 2) -> pd.DataFrame:
    """
    Extrai dados de mudanças de horário de voos, calculando atrasos.
    
    Args:
        conn: Conexão com o banco de dados
        min_changes: Número mínimo de mudanças para incluir o voo na análise
        
    Returns:
        DataFrame com dados de atrasos
    """
    query = """
    WITH changes_count AS (
        SELECT 
            c.unique_flight_id,
            COUNT(*) as total_changes
        FROM 
            flight_changes c
        WHERE 
            c.attribute_changed LIKE '%departure_utc'
        GROUP BY 
            c.unique_flight_id
        HAVING 
            COUNT(*) >= ?
    )
    
    SELECT 
        f.unique_flight_id,
        f.flight_number,
        f.airline_name,
        f.airline_iata,
        f.destination_name,
        cc.total_changes,
        c.attribute_changed,
        c.previous_value,
        c.new_value,
        c.change_detected_cycle_timestamp,
        c.previous_cycle_timestamp
    FROM 
        flight_changes c
    JOIN 
        flight_snapshots f ON c.unique_flight_id = f.unique_flight_id
    JOIN 
        changes_count cc ON c.unique_flight_id = cc.unique_flight_id
    WHERE 
        c.attribute_changed LIKE '%departure_utc'
    ORDER BY 
        c.unique_flight_id,
        c.change_detected_cycle_timestamp
    """
    
    df = pd.read_sql_query(query, conn, params=(min_changes,))
    
    # Convertendo timestamps para datetime e calculando atrasos em minutos
    df['previous_value'] = pd.to_datetime(df['previous_value'])
    df['new_value'] = pd.to_datetime(df['new_value'])
    df['change_detected_cycle_timestamp'] = pd.to_datetime(df['change_detected_cycle_timestamp'])
    
    # Calcular atraso em minutos (positivo = atraso, negativo = adiantamento)
    df['delay_minutes'] = (df['new_value'] - df['previous_value']).dt.total_seconds() / 60
    
    return df

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
    flight_stats = df.groupby(['unique_flight_id', 'flight_number', 'airline_name', 'airline_iata', 'destination_name', 'total_changes']).agg({
        'delay_minutes': ['sum', 'mean', 'max', 'count'],
        'new_value': ['max']  # último horário estimado
    }).reset_index()
    
    # Renomear colunas
    flight_stats.columns = ['unique_flight_id', 'flight_number', 'airline_name', 'airline_iata', 'destination', 
                           'total_changes', 'total_delay_minutes', 'avg_delay_minutes', 
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
    Cria um gráfico de linha mostrando a evolução do horário estimado de partida ao longo do tempo.
    
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
    
    # Criar gráfico
    plt.figure(figsize=(12, 6))
    
    # Plotar linha de evolução do horário estimado
    if 'estimated_departure_utc' in flight_data['attribute_changed'].values:
        est_data = flight_data[flight_data['attribute_changed'] == 'estimated_departure_utc'].sort_values('change_detected_cycle_timestamp')
        plt.plot(est_data['change_detected_cycle_timestamp'], est_data['new_value'], 'o-', label='Horário Estimado')
    
    # Plotar linha de evolução do horário programado
    if 'scheduled_departure_utc' in flight_data['attribute_changed'].values:
        sched_data = flight_data[flight_data['attribute_changed'] == 'scheduled_departure_utc'].sort_values('change_detected_cycle_timestamp')
        plt.plot(sched_data['change_detected_cycle_timestamp'], sched_data['new_value'], 's-', label='Horário Programado')
    
    # Formatar gráfico
    plt.title(f"Evolução de Horários - {flight_name}")
    plt.xlabel("Data/Hora de Detecção da Mudança")
    plt.ylabel("Horário de Partida")
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.legend()
    
    # Formatar eixo x para mostrar data/hora
    plt.gcf().autofmt_xdate()
    
    # Salvar gráfico
    os.makedirs(output_dir, exist_ok=True)
    safe_id = flight_id.replace('/', '_').replace('\\', '_')
    plt.savefig(f"{output_dir}/delay_timeline_{safe_id}.png")
    plt.close()
    
    logfire.info(f"Gráfico de linha criado para o voo {flight_id}")

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
    
    Args:
        df: DataFrame com dados de atrasos
        output_dir: Diretório para salvar o gráfico
    """
    plt.figure(figsize=(12, 6))
    
    # Filtrar valores extremos para melhor visualização
    filtered_delays = df[(df['delay_minutes'] > -120) & (df['delay_minutes'] < 120)]['delay_minutes']
    
    # Criar histograma
    sns.histplot(filtered_delays, kde=True, bins=50)
    
    # Adicionar linha vertical em 0 (sem atraso)
    plt.axvline(x=0, color='red', linestyle='--', alpha=0.7)
    
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
        report += f"   Total de Mudanças: {row.total_changes}\n"
        report += f"   Último Horário Programado: {row.last_scheduled}\n"
        report += f"   Último Horário Estimado: {row.last_estimated}\n\n"
    
    # Salvar relatório
    os.makedirs(output_dir, exist_ok=True)
    with open(f"{output_dir}/most_delayed_flights.txt", 'w') as f:
        f.write(report)
    
    logfire.info("Relatório textual de voos mais atrasados criado")

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
        
        # Gerar gráficos de linha para os 5 voos mais atrasados
        for flight_id in most_delayed['unique_flight_id'].head(5):
            plot_delay_timeline(df, flight_id)
        
        # Gerar heatmap de atrasos
        plot_delay_heatmap(df)
        
        # Gerar comparação entre companhias
        plot_airline_delay_comparison(df)
        
        # Gerar histograma de atrasos
        plot_delay_histogram(df)
        
        # Salvar dados em CSV para análises adicionais
        most_delayed.to_csv(f"{output_dir}/most_delayed_flights.csv", index=False)
        
        logfire.info(f"Relatórios gerados com sucesso e salvos em '{output_dir}'")
        
    finally:
        conn.close()

if __name__ == "__main__":
    import os
    os.makedirs('reports', exist_ok=True)
    run_reports()