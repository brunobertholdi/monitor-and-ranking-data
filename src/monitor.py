"""
[DESCRIPTION]
This module runs the main monitoring loop for flight departures.
It fetches data periodically, compares it with previous snapshots stored in the database,
logs changes, and saves the new snapshots.

[CHANGELOG] - Version - Author - Date - Changes
v0.0.1 - Bruno Bertholdi - 2025-04-19 - Initializes monitoring loop script.
v0.0.2 - Bruno Bertholdi - 2025-04-19 - Fix timestamp comparison logic.
v0.0.3 - Bruno Bertholdi - 2025-04-19 - Introduces cycle_timestamp for grouping.
v0.0.4 - Bruno Bertholdi - 2025-04-20 - Minor refactoring and organization.
"""

# --- Imports --- #
import time
import logfire
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List, Tuple
import sqlite3
import os

# --- Custom Packages --- #
from request import GetData
from database import (
    get_db_connection,
    create_table
)

# --- Utility Functions --- #
def format_timestamp(dt: Optional[datetime]) -> str:
    """
    Formata uma data para string ISO com timezone UTC.

    Args:
        dt: Objeto datetime ou None.

    Returns:
        str: String formatada ou None.
    """
    if dt is None:
        return None
    if isinstance(dt, str):
        return dt
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


# --- Database Functions --- #
def setup_database() -> sqlite3.Connection:
    """
    Configura e retorna uma conexão com o banco de dados.
    Garante que as tabelas necessárias estejam criadas.

    Returns:
        sqlite3.Connection: Conexão ativa com o banco de dados.
    """
    conn = get_db_connection()
    if not conn:
        logfire.error("Falha ao conectar ao banco de dados.")
        return None

    create_table(conn)  # Garante que as tabelas existam
    logfire.info("Banco de dados configurado com sucesso.")
    return conn


def save_snapshot(conn: sqlite3.Connection, snapshot: Dict[str, Any], cycle_timestamp: str) -> None:
    """
    Salva um snapshot do voo na tabela flight_snapshots.

    Args:
        conn: Conexão com o banco de dados.
        snapshot: Dados do voo.
        cycle_timestamp: Timestamp do ciclo atual.
    """
    prepared_data = snapshot.copy()  # Evita modificar o dicionário original
    prepared_data['cycle_timestamp'] = cycle_timestamp
    
    # Usando o mesmo timestamp do ciclo para workspace_timestamp, simplificando
    prepared_data['workspace_timestamp'] = cycle_timestamp

    # Converte objetos datetime para strings ISO se existirem
    for key in ['scheduled_departure_utc', 'estimated_departure_utc']:
        if key in prepared_data:
            prepared_data[key] = format_timestamp(prepared_data.get(key))

    # Converte boolean para inteiro
    if 'is_operator' in prepared_data:
        prepared_data['is_operator'] = 1 if prepared_data['is_operator'] else 0

    # Garante que todas as colunas definidas na tabela existam nos dados
    all_columns = [
        'unique_flight_id', 'cycle_timestamp', 'workspace_timestamp', 'flight_number', 'airline_iata',
        'airline_name', 'scheduled_departure_utc', 'estimated_departure_utc',
        'departure_terminal', 'departure_gate', 'status', 'destination_iata',
        'destination_name', 'codeshare_status', 'is_operator', 'aircraft_model',
        'aircraft_reg'
    ]
    final_data = {col: prepared_data.get(col) for col in all_columns}

    columns = ', '.join(final_data.keys())
    placeholders = ', '.join('?' * len(final_data))
    sql = f"INSERT INTO flight_snapshots ({columns}) VALUES ({placeholders})"

    try:
        cursor = conn.cursor()
        cursor.execute(sql, list(final_data.values()))
        logfire.debug(f"Snapshot salvo para voo: {final_data.get('unique_flight_id')}")
    except sqlite3.Error as e:
        logfire.error(f"Erro ao inserir snapshot: {e}")


def save_change(conn: sqlite3.Connection, change: Dict[str, Any]) -> None:
    """
    Salva uma mudança detectada na tabela flight_changes.

    Args:
        conn: Conexão com o banco de dados.
        change: Dados da mudança.
    """
    change_logged_at = datetime.now(timezone.utc).isoformat()  # Changes logged at UTC
    sql = """
        INSERT INTO flight_changes (
            unique_flight_id, change_detected_cycle_timestamp, previous_cycle_timestamp,
            attribute_changed, previous_value, new_value, change_logged_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
    """

    try:
        cursor = conn.cursor()
        cursor.execute(sql, (
            change['unique_flight_id'],
            change['change_detected_cycle_timestamp'],
            change['previous_cycle_timestamp'],
            change['attribute_changed'],
            str(change['previous_value']),
            str(change['new_value']),
            change_logged_at
        ))
        logfire.info(
            f"Mudança detectada: {change['attribute_changed']} para voo {change['unique_flight_id']}"
        )
    except sqlite3.Error as e:
        logfire.error(f"Erro ao registrar mudança: {e}")


def detect_changes(conn: sqlite3.Connection, current_snapshot: Dict[str, Any],
                   cycle_timestamp: str) -> List[Dict[str, Any]]:
    """
    Detecta mudanças nos dados de um voo comparando com o snapshot anterior.
    Utiliza JULIANDAY para comparação de datas em SQLite.
    Verifica se a mudança já foi registrada antes para evitar duplicações.

    Args:
        conn: Conexão com o banco de dados.
        current_snapshot: Dados atuais do voo.
        cycle_timestamp: Timestamp do ciclo atual.

    Returns:
        List[Dict[str, Any]]: Lista de mudanças detectadas.
    """
    changes = []
    unique_flight_id = current_snapshot.get('unique_flight_id')

    if not unique_flight_id:
        return changes

    # Buscar snapshot anterior
    cursor = conn.cursor()
    cursor.execute("""
        SELECT * FROM flight_snapshots
        WHERE unique_flight_id = ?
        ORDER BY snapshot_id DESC
        LIMIT 1
    """, (unique_flight_id,))

    previous = cursor.fetchone()

    if not previous:
        return changes  # Primeira aparição do voo, sem mudanças

    # 1. Comparar datas de partida programada usando JULIANDAY
    scheduled_prev = previous['scheduled_departure_utc']
    scheduled_curr = current_snapshot.get('scheduled_departure_utc')

    if scheduled_prev and scheduled_curr:
        # Usar parâmetros nomeados para evitar erro de depreciação
        cursor.execute("""
            SELECT ABS(JULIANDAY(:current) - JULIANDAY(:previous)) > 0.00001 as scheduled_changed
        """, {"current": scheduled_curr, "previous": scheduled_prev})

        if cursor.fetchone()[0]:
            # Verificar se esta mudança já foi registrada anteriormente
            cursor.execute("""
                SELECT change_id FROM flight_changes
                WHERE unique_flight_id = ?
                AND attribute_changed = 'scheduled_departure_utc'
                AND new_value = ?
                ORDER BY change_id DESC LIMIT 1
            """, (unique_flight_id, scheduled_curr))

            if cursor.fetchone() is None:  # Só adicionar se não encontrar registro idêntico
                changes.append({
                    'unique_flight_id': unique_flight_id,
                    'change_detected_cycle_timestamp': cycle_timestamp,
                    'previous_cycle_timestamp': previous['cycle_timestamp'],
                    'attribute_changed': 'scheduled_departure_utc',
                    'previous_value': scheduled_prev,
                    'new_value': scheduled_curr
                })

    # 2. Comparar datas estimadas de partida usando JULIANDAY
    estimated_prev = previous['estimated_departure_utc']
    estimated_curr = current_snapshot.get('estimated_departure_utc')

    if estimated_prev and estimated_curr:
        # Usar parâmetros nomeados para evitar erro de depreciação
        cursor.execute("""
            SELECT ABS(JULIANDAY(:current) - JULIANDAY(:previous)) > 0.00001 as estimated_changed
        """, {"current": estimated_curr, "previous": estimated_prev})

        if cursor.fetchone()[0]:
            # Verificar se esta mudança já foi registrada anteriormente
            cursor.execute("""
                SELECT change_id FROM flight_changes
                WHERE unique_flight_id = ?
                AND attribute_changed = 'estimated_departure_utc'
                AND new_value = ?
                ORDER BY change_id DESC LIMIT 1
            """, (unique_flight_id, estimated_curr))

            if cursor.fetchone() is None:  # Só adicionar se não encontrar registro idêntico
                changes.append({
                    'unique_flight_id': unique_flight_id,
                    'change_detected_cycle_timestamp': cycle_timestamp,
                    'previous_cycle_timestamp': previous['cycle_timestamp'],
                    'attribute_changed': 'estimated_departure_utc',
                    'previous_value': estimated_prev,
                    'new_value': estimated_curr
                })

    # 3. Comparar portão de embarque
    gate_prev = previous['departure_gate']
    gate_curr = current_snapshot.get('departure_gate')

    # Normalizar valores nulos ou vazios
    gate_prev_norm = gate_prev if gate_prev and gate_prev.strip() else None
    gate_curr_norm = gate_curr if gate_curr and gate_curr.strip() else None

    if gate_prev_norm != gate_curr_norm:
        # Verificar se esta mudança já foi registrada anteriormente
        cursor.execute("""
            SELECT change_id FROM flight_changes
            WHERE unique_flight_id = ?
            AND attribute_changed = 'departure_gate'
            AND new_value = ?
            ORDER BY change_id DESC LIMIT 1
        """, (unique_flight_id, gate_curr))

        if cursor.fetchone() is None:  # Só adicionar se não encontrar registro idêntico
            changes.append({
                'unique_flight_id': unique_flight_id,
                'change_detected_cycle_timestamp': cycle_timestamp,
                'previous_cycle_timestamp': previous['cycle_timestamp'],
                'attribute_changed': 'departure_gate',
                'previous_value': gate_prev,
                'new_value': gate_curr
            })

    return changes


def fetch_flight_data() -> List[Dict[str, Any]]:
    """
    Consulta a API para obter os dados mais recentes de voos.
    Normaliza e valida os dados antes de retorná-los.

    Returns:
        List[Dict[str, Any]]: Lista de dicionários contendo dados dos voos.
    """
    data_getter = GetData()
    flight_data = data_getter.make_request()

    if not flight_data:
        logfire.warning("Nenhum dado de voo retornado pela API.")
        return []

    # Converte modelos Pydantic para dicionários, se necessário
    return [snapshot.dict() if hasattr(snapshot, 'dict') else snapshot for snapshot in flight_data]


def process_flight_data(conn: sqlite3.Connection, flights: List[Dict[str, Any]], 
                         cycle_timestamp: str) -> int:
    """
    Processa os dados de voos, detecta mudanças e salva snapshots.

    Args:
        conn: Conexão com o banco de dados.
        flights: Lista de dicionários com dados dos voos.
        cycle_timestamp: Timestamp do ciclo atual.

    Returns:
        int: Número de mudanças detectadas.
    """
    total_changes = 0

    for flight in flights:
        # Verificar dados obrigatórios
        if not flight.get('unique_flight_id'):
            logfire.debug("Ignorando registro sem unique_flight_id.")
            continue

        # Detectar mudanças
        changes = detect_changes(conn, flight, cycle_timestamp)

        # Salvar mudanças detectadas
        for change in changes:
            save_change(conn, change)
            total_changes += 1

        # Sempre salvar o novo snapshot
        save_snapshot(conn, flight, cycle_timestamp)

    return total_changes



def run_monitor_cycle(conn: sqlite3.Connection) -> int:
    """
    Executa um ciclo completo de monitoramento:
    busca dados, detecta mudanças e salva snapshots.

    Args:
        conn: Conexão com o banco de dados.

    Returns:
        int: Número de mudanças detectadas neste ciclo.
    """
    # Gerar o timestamp para este ciclo
    current_cycle_timestamp = datetime.now(timezone.utc).isoformat()
    logfire.info(f"Iniciando ciclo de monitoramento em {current_cycle_timestamp}")

    # Buscar dados atualizados dos voos
    flights = fetch_flight_data()

    if not flights:
        logfire.warning("Nenhum dado de voo disponível neste ciclo.")
        return 0

    logfire.info(f"Obtidos {len(flights)} registros de voos.")

    # Processar dados e detectar mudanças
    total_changes = process_flight_data(conn, flights, current_cycle_timestamp)

    # Commit das alterações
    try:
        conn.commit()
        logfire.info(f"Ciclo finalizado com {total_changes} mudanças detectadas.")
    except sqlite3.Error as e:
        logfire.error(f"Erro ao fazer commit das alterações: {e}")
        conn.rollback()
        return 0

    return total_changes
def run_monitor(max_cycles: int = 80, interval_seconds: int = 120) -> None:
    """
    Função principal que executa o loop de monitoramento.

    Args:
        max_cycles: Número máximo de ciclos a serem executados.
        interval_seconds: Intervalo entre os ciclos em segundos.
    """
    logfire.info("--- Iniciando Monitor de Voos ---")

    # Configurar banco de dados
    conn = setup_database()
    if not conn:
        logfire.error("Não foi possível configurar o banco de dados. Saindo.")
        return

    logfire.info(f"Iniciando monitoramento por {max_cycles} ciclos (aproximadamente {max_cycles//60} horas) com intervalo de {interval_seconds} segundos.")

    cycle_num = 0
    try:
        for cycle_num in range(1, max_cycles + 1):
            start_time = time.time()

            logfire.info(f"Iniciando ciclo {cycle_num}/{max_cycles}")

            try:
                # Executar um ciclo completo de monitoramento
                changes = run_monitor_cycle(conn)
                logfire.info(f"Ciclo {cycle_num}: {changes} mudanças detectadas")

                # Se for o primeiro ciclo, adicionar uma mensagem específica
                if cycle_num == 1:
                    logfire.info("Primeiro ciclo completo - estabelecendo linha de base para comparações futuras")

            except Exception as e:
                logfire.error(f"Erro durante o ciclo de monitoramento: {e}")

            # Calcular tempo de espera para o próximo ciclo
            end_time = time.time()
            elapsed_time = end_time - start_time
            wait_time = max(0, interval_seconds - elapsed_time)

            logfire.info(f"Ciclo {cycle_num} levou {elapsed_time:.2f}s. Aguardando {wait_time:.2f}s para o próximo ciclo.")

            # Apenas aguardar se não for o último ciclo
            if cycle_num < max_cycles:
                try:
                    time.sleep(wait_time)
                except KeyboardInterrupt:
                    logfire.info(f"Monitor interrompido pelo usuário após {cycle_num} ciclos completos")
                    break

    except KeyboardInterrupt:
        logfire.info(f"Monitor interrompido pelo usuário durante o ciclo {cycle_num}")
    finally:
        # Limpeza
        logfire.info(f"--- Completados {cycle_num} ciclos de monitoramento ---")
        if conn:
            conn.close()
            logfire.info("Conexão com o banco de dados fechada.")

# --- Main Execution ---
if __name__ == "__main__":
    # Configura o Logfire usando variável de ambiente se disponível
    try:
        token = os.getenv("LOGFIRE_TOKEN")
        if token:
            logfire.configure(token=token)
        else:
            logfire.configure()
        logfire.info("Logfire configurado com sucesso.")
    except Exception as e:
        print(f"Aviso: Não foi possível configurar o Logfire: {e}")
    
    # Iniciar o monitor com os parâmetros padrão
    run_monitor()