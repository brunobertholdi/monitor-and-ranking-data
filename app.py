import streamlit as st
import pandas as pd
import os

# Configuração da página
st.set_page_config(
    page_title="Monitor de Voos DFW - Dashboard",
    page_icon="✈️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Título do dashboard
st.title("Dashboard de Monitoramento e Ranking de Voos")
st.markdown("### Aeroporto Internacional de Dallas/Fort Worth (DFW)")

# Verifica se a pasta reports existe
reports_dir = "reports"
if not os.path.exists(reports_dir):
    st.error(f"Pasta {reports_dir} não encontrada. Execute reports.py e ranking.py para gerar relatórios primeiro.")
    st.stop()

# Verifica se os relatórios principais existem
required_files = [
    "delay_histogram.png",
    "delay_heatmap.png", 
    "consolidated_delays.png",
    "delay_evolution.png",
    "overall_ranking.png",
    "time_changes_ranking.png",
    "gate_changes_ranking.png"
]

missing_files = [f for f in required_files if not os.path.exists(os.path.join(reports_dir, f))]
if missing_files:
    st.warning(f"Alguns relatórios estão faltando: {', '.join(missing_files)}")
    st.info("Execute src/reports.py e src/ranking.py para gerar todos os relatórios.")

# Carrega CSV de rankings se disponível
airline_rankings_df = None
if os.path.exists(os.path.join(reports_dir, "airline_rankings.csv")):
    airline_rankings_df = pd.read_csv(os.path.join(reports_dir, "airline_rankings.csv"))

# Sidebar
st.sidebar.header("Navegação")
page = st.sidebar.radio(
    "Selecione uma seção:",
    ["Visão Geral", "Análise de Atrasos", "Ranking de Companhias", "Detalhes por Voo"]
)

# Visão Geral
if page == "Visão Geral":
    st.header("Visão Geral do Sistema de Monitoramento")
    
    # Informações sobre o sistema
    st.subheader("Sobre o Sistema")
    st.markdown("""
    Este dashboard apresenta análises e visualizações dos dados coletados pelo Sistema de Monitoramento de Voos de DFW. 
    O sistema monitora partidas de voos em tempo real, registra mudanças em horários e portões, e gera análises sobre o desempenho das companhias aéreas.
    
    ### Principais recursos deste dashboard:
    - **Visão Geral**: Informações sobre o sistema de monitoramento
    - **Análise de Atrasos**: Visualizações de atrasos por diferentes dimensões
    - **Ranking de Companhias**: Classificação das companhias aéreas por mudanças
    - **Detalhes por Voo**: Análise detalhada de atrasos por voo
    """)
    
    # Estatísticas e números chave, se disponíveis
    if airline_rankings_df is not None:
        st.subheader("Estatísticas Chave")
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total de Companhias Monitoradas", len(airline_rankings_df))
        with col2:
            st.metric("Companhia com Mais Mudanças", 
                     airline_rankings_df.iloc[0]['airline_iata'] if not airline_rankings_df.empty else "N/A")
        with col3:
            total_changes = airline_rankings_df['total_changes'].sum() if not airline_rankings_df.empty else 0
            st.metric("Total de Mudanças Detectadas", total_changes)

# Análise de Atrasos
elif page == "Análise de Atrasos":
    st.header("Análise de Atrasos de Voos")
    
    tab1, tab2 = st.tabs(["Distribuição de Atrasos", "Heatmap por Horário"])
    
    with tab1:
        st.subheader("Distribuição dos Atrasos")
        delay_histogram_path = os.path.join(reports_dir, "delay_histogram.png")
        if os.path.exists(delay_histogram_path):
            st.image(delay_histogram_path)
            st.caption("Histograma mostrando a distribuição dos atrasos em minutos. Apenas atrasos de 5 minutos ou mais são considerados.")
        else:
            st.warning("Histograma de atrasos não encontrado. Execute reports.py para gerar.")
    
    with tab2:
        st.subheader("Heatmap de Atrasos por Dia e Hora")
        delay_heatmap_path = os.path.join(reports_dir, "delay_heatmap.png")
        if os.path.exists(delay_heatmap_path):
            st.image(delay_heatmap_path)
            st.caption("Heatmap mostrando a intensidade dos atrasos por dia da semana e hora do dia.")
        else:
            st.warning("Heatmap de atrasos não encontrado. Execute reports.py para gerar.")

# Ranking de Companhias
elif page == "Ranking de Companhias":
    st.header("Ranking de Companhias Aéreas")
    
    tab1, tab2, tab3, tab4 = st.tabs(["Ranking Geral", "Mudanças de Horário", "Mudanças de Portão", "Relatório Textual"])
    
    with tab1:
        st.subheader("Top 10 Companhias por Total de Mudanças")
        overall_ranking_path = os.path.join(reports_dir, "overall_ranking.png")
        if os.path.exists(overall_ranking_path):
            st.image(overall_ranking_path)
        elif airline_rankings_df is not None:
            # Mostra tabela se a imagem não está disponível mas temos os dados
            st.table(airline_rankings_df.sort_values('total_changes', ascending=False).head(10)[["airline_iata", "airline_name", "total_changes"]])
        else:
            st.warning("Ranking geral não disponível. Execute ranking.py para gerar.")
    
    with tab2:
        st.subheader("Top 10 Companhias por Mudanças de Horário")
        time_ranking_path = os.path.join(reports_dir, "time_changes_ranking.png")
        if os.path.exists(time_ranking_path):
            st.image(time_ranking_path)
        elif airline_rankings_df is not None:
            st.table(airline_rankings_df.sort_values('time_changes', ascending=False).head(10)[["airline_iata", "airline_name", "time_changes"]])
        else:
            st.warning("Ranking de mudanças de horário não disponível. Execute ranking.py para gerar.")
    
    with tab3:
        st.subheader("Top 10 Companhias por Mudanças de Portão")
        gate_ranking_path = os.path.join(reports_dir, "gate_changes_ranking.png")
        if os.path.exists(gate_ranking_path):
            st.image(gate_ranking_path)
        elif airline_rankings_df is not None:
            st.table(airline_rankings_df.sort_values('gate_changes', ascending=False).head(10)[["airline_iata", "airline_name", "gate_changes"]])
        else:
            st.warning("Ranking de mudanças de portão não disponível. Execute ranking.py para gerar.")
    
    with tab4:
        st.subheader("Relatório Detalhado de Rankings")
        ranking_text_path = os.path.join(reports_dir, "airline_rankings.txt")
        if os.path.exists(ranking_text_path):
            with open(ranking_text_path, 'r') as f:
                report = f.read()
                st.text(report)
        else:
            st.warning("Relatório textual não disponível. Execute ranking.py para gerar.")

# Detalhes por Voo
elif page == "Detalhes por Voo":
    st.header("Análise Detalhada de Atrasos por Voo")
    
    # Seleção de visualização
    view_type = st.radio(
        "Selecione o tipo de visualização:",
        ["Voos mais atrasados", "Evolução de atrasos por voo"]
    )
    
    if view_type == "Voos mais atrasados":
        st.subheader("Top Voos com Maiores Atrasos")
        consolidated_delays_path = os.path.join(reports_dir, "consolidated_delays.png")
        if os.path.exists(consolidated_delays_path):
            st.image(consolidated_delays_path)
            st.caption("Gráfico mostrando os voos com maiores atrasos detectados pelo sistema.")
        else:
            st.warning("Gráfico de voos mais atrasados não disponível. Execute reports.py para gerar.")
    
    else:  # Evolução de atrasos
        st.subheader("Evolução dos Atrasos ao Longo do Tempo")
        delay_evolution_path = os.path.join(reports_dir, "delay_evolution.png")
        if os.path.exists(delay_evolution_path):
            st.image(delay_evolution_path)
            st.caption("Gráfico mostrando como os atrasos evoluem ao longo do tempo para os principais voos afetados.")
        else:
            st.warning("Gráfico de evolução de atrasos não disponível. Execute reports.py para gerar.")

# Rodapé
st.markdown("---")
st.markdown("""
<div style='text-align: center'>
    <p>Sistema de Monitoramento e Ranking de Voos DFW - Desenvolvido com Python, SQLite e Streamlit</p>
</div>
""", unsafe_allow_html=True)
