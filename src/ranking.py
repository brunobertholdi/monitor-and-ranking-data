# ranking.py
import sqlite3
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from typing import Dict, List, Tuple, Optional
import logfire
from database import get_db_connection

def extract_airline_changes(conn: sqlite3.Connection) -> pd.DataFrame:
    """
    Extrai todas as mudanças da tabela flight_changes e as associa às companhias aéreas.
    Evita a duplicação de mudanças usando DISTINCT na consulta e garantindo
    que cada mudança (change_id) seja contado apenas uma vez.
    """
    # Conte o número total de registros na tabela flight_changes para referência
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM flight_changes")
    total_changes = cursor.fetchone()[0]
    print(f"Total de registros na tabela flight_changes: {total_changes}")

    query = """
    SELECT
        fs.airline_iata,
        fs.airline_name,
        fc.attribute_changed,
        fc.previous_value,
        fc.new_value,
        fc.change_detected_cycle_timestamp,
        fc.change_id  -- Incluindo change_id para garantir unicidade
    FROM
        flight_changes fc
    JOIN (
        -- Subconsulta para selecionar apenas um snapshot por unique_flight_id
        -- usando o snapshot mais recente para cada voo
        SELECT
            unique_flight_id,
            airline_iata,
            airline_name,
            MAX(snapshot_id) as latest_snapshot_id
        FROM
            flight_snapshots
        WHERE
            airline_iata IS NOT NULL
        GROUP BY
            unique_flight_id, airline_iata, airline_name
    ) fs ON fc.unique_flight_id = fs.unique_flight_id
    """

    df = pd.read_sql_query(query, conn)
    print(f"Número de mudanças extraídas após correção: {len(df)}\n")
    return df

def calculate_time_change_ranking(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula o ranking das companhias aéreas por mudanças nos horários.
    """
    # Filtrar apenas mudanças de horário (scheduled ou estimated)
    time_changes = df[df['attribute_changed'].str.contains('_departure_utc')]

    # Agrupar por companhia aérea e contar
    airline_counts = time_changes.groupby(['airline_iata', 'airline_name']).size().reset_index(name='time_changes')

    # Calcular ranking
    airline_counts['time_change_rank'] = airline_counts['time_changes'].rank(ascending=False, method='dense')

    return airline_counts.sort_values('time_change_rank')

def calculate_gate_change_ranking(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula o ranking das companhias aéreas por mudanças nas portas de embarque.
    """
    # Filtrar apenas mudanças de portão
    gate_changes = df[df['attribute_changed'] == 'departure_gate']

    # Agrupar por companhia aérea e contar
    airline_counts = gate_changes.groupby(['airline_iata', 'airline_name']).size().reset_index(name='gate_changes')

    # Calcular ranking
    airline_counts['gate_change_rank'] = airline_counts['gate_changes'].rank(ascending=False, method='dense')

    return airline_counts.sort_values('gate_change_rank')

def calculate_overall_ranking(time_df: pd.DataFrame, gate_df: pd.DataFrame) -> pd.DataFrame:
    """
    Combina os rankings de tempo e portão para criar um ranking geral.
    """
    # Mesclar os dataframes
    merged = pd.merge(time_df, gate_df, on=['airline_iata', 'airline_name'], how='outer').fillna(0)

    # Calcular pontuação geral (menor é melhor)
    merged['total_changes'] = merged['time_changes'] + merged['gate_changes']
    merged['overall_rank'] = merged['total_changes'].rank(ascending=False, method='dense')

    return merged.sort_values('overall_rank')

def generate_summary_report(df: pd.DataFrame) -> str:
    """
    Gera um relatório textual resumido dos rankings.
    """
    report = "RANKING DE COMPANHIAS AÉREAS POR MUDANÇAS\n"
    report += "=" * 50 + "\n\n"

    report += "TOP 5 - MUDANÇAS DE HORÁRIO:\n"
    top_time = df.sort_values('time_change_rank').head(5)
    for _, row in top_time.iterrows():
        report += f"{int(row['time_change_rank'])}. {row['airline_name']} ({row['airline_iata']}): {int(row['time_changes'])} mudanças\n"

    report += "\nTOP 5 - MUDANÇAS DE PORTÃO:\n"
    top_gate = df.sort_values('gate_change_rank').head(5)
    for _, row in top_gate.iterrows():
        report += f"{int(row['gate_change_rank'])}. {row['airline_name']} ({row['airline_iata']}): {int(row['gate_changes'])} mudanças\n"

    report += "\nTOP 5 - RANKING GERAL (MAIOR NÚMERO DE MUDANÇAS):\n"
    top_overall = df.sort_values('overall_rank').head(5)
    for _, row in top_overall.iterrows():
        report += f"{int(row['overall_rank'])}. {row['airline_name']} ({row['airline_iata']}): {int(row['total_changes'])} mudanças totais\n"

    return report

def plot_rankings(df: pd.DataFrame, output_dir: str = 'reports'):
    """
    Gera visualizações dos rankings.
    """
    import os
    os.makedirs(output_dir, exist_ok=True)

    # Plot 1: Mudanças de horário por companhia aérea
    plt.figure(figsize=(12, 8))
    time_plot = df.sort_values('time_changes', ascending=False).head(10)
    sns.barplot(x='airline_iata', y='time_changes', data=time_plot)
    plt.title('Top 10 Companhias Aéreas por Mudanças de Horário')
    plt.tight_layout()
    plt.savefig(f'{output_dir}/time_changes_ranking.png')

    # Plot 2: Mudanças de portão por companhia aérea
    plt.figure(figsize=(12, 8))
    gate_plot = df.sort_values('gate_changes', ascending=False).head(10)
    sns.barplot(x='airline_iata', y='gate_changes', data=gate_plot)
    plt.title('Top 10 Companhias Aéreas por Mudanças de Portão')
    plt.tight_layout()
    plt.savefig(f'{output_dir}/gate_changes_ranking.png')

    # Plot 3: Ranking geral por companhia aérea
    plt.figure(figsize=(12, 8))
    overall_plot = df.sort_values('total_changes', ascending=False).head(10)
    sns.barplot(x='airline_iata', y='total_changes', data=overall_plot)
    plt.title('Top 10 Companhias Aéreas por Total de Mudanças')
    plt.tight_layout()
    plt.savefig(f'{output_dir}/overall_ranking.png')

    logfire.info(f"Visualizações salvas no diretório '{output_dir}'.")

def run_ranking_analysis(db_path: Optional[str] = None):
    """
    Executa a análise completa de ranking e gera relatórios.
    """
    conn = get_db_connection() if db_path is None else sqlite3.connect(db_path)

    try:
        logfire.info("Iniciando análise de ranking de companhias aéreas...")

        # Extrair dados
        df = extract_airline_changes(conn)

        if df.empty:
            logfire.warning("Não há dados suficientes para gerar rankings.")
            return

        # Calcular rankings
        time_ranking = calculate_time_change_ranking(df)
        gate_ranking = calculate_gate_change_ranking(df)
        overall_ranking = calculate_overall_ranking(time_ranking, gate_ranking)

        # Gerar relatórios
        report = generate_summary_report(overall_ranking)
        print(report)

        # Salvar relatório em arquivo
        with open('reports/airline_rankings.txt', 'w') as f:
            f.write(report)

        # Gerar visualizações
        plot_rankings(overall_ranking)

        # Salvar dados em CSV para análises adicionais
        overall_ranking.to_csv('reports/airline_rankings.csv', index=False)

        logfire.info("Análise de ranking concluída com sucesso.")

    finally:
        conn.close()

if __name__ == "__main__":
    import os
    os.makedirs('reports', exist_ok=True)
    run_ranking_analysis()