import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import plotly.graph_objects as go
import sqlite3
import os
from datetime import datetime
import logging
import traceback
from threading import Lock

# Nastavení logování
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Konfigurace Streamlit
st.set_page_config(page_title="Prohlížeč dat o suchu", layout="wide")

# Konstanty
SQL_FILE_PATH = r"D:\OneDrive - CZU v Praze\SPS\Projekty\Agrometeorologie_cz\Sucho\Data\Save\Output\pozemky_data.sql"
DATABASE_NAME = "sucho_database.db"

# Inicializace session state
if 'conn' not in st.session_state:
    st.session_state.conn = None
    st.session_state.lock = Lock()

def create_db_from_sql():
    try:
        conn = sqlite3.connect(DATABASE_NAME, check_same_thread=False)
        with open(SQL_FILE_PATH, 'r', encoding='utf-8') as sql_file:
            sql_script = sql_file.read()
        with st.session_state.lock:
            conn.executescript(sql_script)
        logger.info("Databáze úspěšně vytvořena z SQL souboru")
        return conn
    except Exception as e:
        logger.error(f"Chyba při vytváření databáze z SQL: {e}")
        st.error(f"Nepodařilo se vytvořit databázi z SQL: {e}")
        return None

@st.cache_data
def get_unique_values(_conn, column, filters=None):
    query = f"SELECT DISTINCT {column} FROM pozemky_data WHERE {column} IS NOT NULL AND {column} != ''"
    params = []
    if filters:
        for key, values in filters.items():
            if values:
                query += f" AND {key} IN ({','.join(['?'] * len(values))})"
                params.extend(values)
    
    try:
        with st.session_state.lock:
            df = pd.read_sql(query, _conn, params=params)
        return sorted(df[column].tolist())
    except sqlite3.Error as e:
        logger.error(f"Chyba při získávání unikátních hodnot pro {column}: {e}")
        st.error(f"Nepodařilo se získat unikátní hodnoty pro {column}: {e}")
        return []

def load_data(_conn, zkod_dpb=None, id_uz=None, ku_kod=None, okres_kod=None, date_from=None, date_to=None, drought_level=None):
    query = "SELECT * FROM pozemky_data WHERE 1=1"
    params = []

    if zkod_dpb:
        query += f" AND ZKOD_DPB IN ({','.join(['?']*len(zkod_dpb))})"
        params.extend(zkod_dpb)
    if id_uz:
        query += f" AND ID_UZ IN ({','.join(['?']*len(id_uz))})"
        params.extend(id_uz)
    if ku_kod:
        query += f" AND KU_KOD IN ({','.join(['?']*len(ku_kod))})"
        params.extend(ku_kod)
    if okres_kod:
        query += f" AND OKRES_KOD IN ({','.join(['?']*len(okres_kod))})"
        params.extend(okres_kod)

    try:
        with st.session_state.lock:
            df = pd.read_sql(query, _conn, params=params)

        if date_from and date_to:
            date_from_str = date_from.strftime('%Y%m%d')
            date_to_str = date_to.strftime('%Y%m%d')
            m_columns = [col for col in df.columns if col.startswith('M_') and date_from_str <= col.split('_')[1] <= date_to_str]
        else:
            m_columns = [col for col in df.columns if col.startswith('M_')]

        df = df[['ZKOD_DPB', 'ID_UZ', 'KU_KOD', 'OKRES_KOD'] + m_columns]

        for col in m_columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        df['mean'] = df[m_columns].mean(axis=1)

        if drought_level is not None:
            df = df[df['mean'] <= drought_level]

        return df
    except sqlite3.Error as e:
        logger.error(f"Chyba při načítání dat: {e}")
        st.error(f"Nepodařilo se načíst data: {e}")
        return pd.DataFrame()

def visualize_data(df):
    if df.empty:
        st.write("Žádná data k zobrazení.")
        return

    m_columns = [col for col in df.columns if col.startswith('M_')]
    
    # Časový vývoj sucha
    st.subheader('Časový vývoj sucha')
    df_long = df.melt(id_vars=['ZKOD_DPB'], value_vars=m_columns, var_name='date', value_name='drought_level')
    df_long['date'] = pd.to_datetime(df_long['date'].str.split('_').str[1], format='%Y%m%d')
    
    fig = px.line(df_long, x='date', y='drought_level', color='ZKOD_DPB',
                  labels={'date': 'Datum', 'drought_level': 'Úroveň sucha', 'ZKOD_DPB': 'ZKOD_DPB'},
                  title='Časový vývoj sucha')
    st.plotly_chart(fig, use_container_width=True)
    st.write("""
    Tento graf zobrazuje vývoj úrovně sucha v čase pro různé pozemky (ZKOD_DPB).
    - Osa X představuje časové období
    - Osa Y představuje úroveň sucha (0-5, kde 5 je extrémní sucho)
    - Každá barevná čára reprezentuje jeden pozemek
    """)

    # Distribuce úrovně sucha
    st.subheader('Distribuce úrovně sucha')
    fig = px.histogram(df, x='mean', nbins=50,
                       labels={'mean': 'Průměrná úroveň sucha', 'count': 'Počet pozemků'},
                       title='Distribuce průměrné úrovně sucha')
    fig.update_xaxes(range=[0, 5])
    st.plotly_chart(fig, use_container_width=True)
    st.write("""
    Tento histogram ukazuje rozložení průměrné úrovně sucha napříč všemi pozemky.
    - Osa X představuje úroveň sucha (0-5)
    - Osa Y představuje počet pozemků
    - Vrcholy grafu ukazují nejčastěji se vyskytující úrovně sucha
    """)

    # Úroveň sucha podle okresů
    st.subheader('Úroveň sucha podle okresů')
    df_okres = df.groupby('OKRES_KOD')['mean'].agg(['mean', 'min', 'max']).reset_index()
    df_okres = df_okres.sort_values('mean', ascending=False)
    
    fig = go.Figure()
    fig.add_trace(go.Bar(x=df_okres['OKRES_KOD'], y=df_okres['mean'], name='Průměr',
                         error_y=dict(type='data', symmetric=False,
                                      array=df_okres['max']-df_okres['mean'],
                                      arrayminus=df_okres['mean']-df_okres['min'])))
    
    fig.update_layout(title='Průměrná úroveň sucha podle okresů s rozsahem',
                      xaxis_title='Kód okresu',
                      yaxis_title='Úroveň sucha',
                      yaxis_range=[0, 5])
    
    st.plotly_chart(fig, use_container_width=True)
    st.write("""
    Tento graf zobrazuje průměrnou úroveň sucha pro každý okres.
    - Sloupce představují průměrnou úroveň sucha
    - Chybové úsečky ukazují rozsah od minimální po maximální hodnotu v daném okrese
    """)

def calculate_statistics(df):
    m_columns = [col for col in df.columns if col.startswith('M_')]
    
    stats = {
        'Průměrná úroveň sucha': df['mean'].mean(),
        'Medián úrovně sucha': df['mean'].median(),
        'Počet pozemků v kritickém suchu (> 4)': (df['mean'] > 4).sum(),
        'Procento pozemků v kritickém suchu': (df['mean'] > 4).mean() * 100,
        'Nejsušší okres': df.groupby('OKRES_KOD')['mean'].mean().idxmax(),
        'Nejvlhčí okres': df.groupby('OKRES_KOD')['mean'].mean().idxmin(),
        'Trend sucha': 'Rostoucí' if df[m_columns].mean().is_monotonic_increasing else 'Klesající'
    }
    
    stats_df = pd.DataFrame.from_dict(stats, orient='index', columns=['Hodnota'])
    stats_df['Hodnota'] = stats_df['Hodnota'].apply(lambda x: round(x, 2) if isinstance(x, (int, float)) else x)
    
    return stats_df

def display_statistics(stats_df):
    st.subheader('Statistiky sucha')
    st.dataframe(stats_df)
    
    st.write("""
    Vysvětlení statistik:
    - Průměrná úroveň sucha: Průměrná hodnota sucha napříč všemi pozemky (0-5).
    - Medián úrovně sucha: Střední hodnota úrovně sucha (50% pozemků má nižší hodnotu, 50% vyšší).
    - Počet pozemků v kritickém suchu: Kolik pozemků má úroveň sucha vyšší než 4.
    - Procento pozemků v kritickém suchu: Jaké procento pozemků je v kritickém suchu.
    - Nejsušší okres: Okres s nejvyšší průměrnou úrovní sucha.
    - Nejvlhčí okres: Okres s nejnižší průměrnou úrovní sucha.
    - Trend sucha: Zda úroveň sucha celkově roste nebo klesá v průběhu času.
    """)

    # Interpretace výsledků
    avg_drought = stats_df.loc['Průměrná úroveň sucha', 'Hodnota']
    critical_percent = stats_df.loc['Procento pozemků v kritickém suchu', 'Hodnota']
    trend = stats_df.loc['Trend sucha', 'Hodnota']

    st.write(f"""
    Interpretace výsledků:
    - Průměrná úroveň sucha je {avg_drought:.2f} na škále 0-5, což znamená 
      {"velmi vážnou situaci" if avg_drought > 4 else "vážnou situaci" if avg_drought > 3 else "střední úroveň sucha" if avg_drought > 2 else "mírné sucho" if avg_drought > 1 else "normální stav"}.
    - {critical_percent:.2f}% pozemků se nachází v kritickém suchu (úroveň > 4), což je 
      {"alarmující" if critical_percent > 50 else "znepokojující" if critical_percent > 25 else "významné" if critical_percent > 10 else "nízké"} procento.
    - Celkový trend sucha je {trend.lower()}, což naznačuje, že situace se 
      {"zhoršuje" if trend == "Rostoucí" else "zlepšuje"} v průběhu času.
    """)

def analyze_drought_days(df):
    m_columns = [col for col in df.columns if col.startswith('M_')]
    
    drought_levels = {
        'Normální stav (0-1)': (0, 1),
        'Mírné sucho (1-2)': (1, 2),
        'Střední sucho (2-3)': (2, 3),
        'Vážné sucho (3-4)': (3, 4),
        'Extrémní sucho (4-5)': (4, 5)
    }
    
    results = {}
    for level, (min_val, max_val) in drought_levels.items():
        days = ((df[m_columns] > min_val) & (df[m_columns] <= max_val)).sum().sum()
        results[level] = days
    
    results_df = pd.DataFrame.from_dict(results, orient='index', columns=['Počet dnů'])
    results_df = results_df.sort_values('Počet dnů', ascending=False)
    
    st.subheader('Analýza počtu dnů s různými úrovněmi sucha')
    st.dataframe(results_df)
    
    fig = px.bar(results_df, x=results_df.index, y='Počet dnů',
                 labels={'index': 'Úroveň sucha', 'Počet dnů': 'Počet dnů'},
                 title='Počet dnů s různými úrovněmi sucha')
    st.plotly_chart(fig, use_container_width=True)
    
    st.write("""
    Tento graf a tabulka ukazují, kolik dnů bylo zaznamenáno pro každou úroveň sucha napříč všemi pozemky a celým časovým obdobím.
    To poskytuje přehled o tom, jak často se vyskytují různé úrovně sucha.
             """)

def main():
    st.title('Prohlížeč dat o suchu')
    st.sidebar.image("http://agropocasi.cz/wp-content/uploads/2022/11/Logo-45.png", use_column_width=True)

    try:
        # Vytvoření nebo připojení k databázi
        if st.session_state.conn is None:
            with st.spinner('Vytváření databáze ze SQL souboru...'):
                st.session_state.conn = create_db_from_sql()
        
        if st.session_state.conn is None:
            st.error("Nepodařilo se vytvořit spojení s databází. Aplikace nemůže pokračovat.")
            return

        st.sidebar.header('Filtry')
        
        def selectbox_with_search(label, options, key):
            search = st.sidebar.text_input(f"Hledat {label}", key=f"search_{key}")
            filtered_options = [opt for opt in options if search.lower() in str(opt).lower()]
            return st.sidebar.multiselect(label, filtered_options, key=key)

        filters = {}
        
        with st.spinner('Načítání možností filtrů...'):
            zkod_dpb_options = get_unique_values(st.session_state.conn, 'ZKOD_DPB', filters)
            zkod_dpb = selectbox_with_search('ZKOD_DPB:', zkod_dpb_options, 'zkod_dpb')
            filters['ZKOD_DPB'] = zkod_dpb

            id_uz_options = get_unique_values(st.session_state.conn, 'ID_UZ', filters)
            id_uz = selectbox_with_search('ID_UZ:', id_uz_options, 'id_uz')
            filters['ID_UZ'] = id_uz

            ku_kod_options = get_unique_values(st.session_state.conn, 'KU_KOD', filters)
            ku_kod = selectbox_with_search('KU_KOD:', ku_kod_options, 'ku_kod')
            filters['KU_KOD'] = ku_kod

            okres_kod_options = get_unique_values(st.session_state.conn, 'OKRES_KOD', filters)
            okres_kod = selectbox_with_search('OKRES_KOD:', okres_kod_options, 'okres_kod')
        
        col1, col2 = st.sidebar.columns(2)
        with col1:
            date_from = st.date_input('Od data:', min_value=datetime(1900, 1, 1), max_value=datetime.now())
        with col2:
            date_to = st.date_input('Do data:', min_value=datetime(1900, 1, 1), max_value=datetime.now())
        
        drought_level = st.sidebar.slider('Maximální úroveň sucha:', 0.0, 5.0, 5.0, 0.1)

        if st.sidebar.button('Filtrovat a zobrazit', key='filter_button'):
            with st.spinner('Načítání a zpracování dat...'):
                df = load_data(st.session_state.conn, zkod_dpb, id_uz, ku_kod, okres_kod, date_from, date_to, drought_level)
            
            if not df.empty:
                st.success(f'Načteno {len(df)} záznamů.')
                
                # Příprava dat pro stažení
                csv = df.to_csv(index=False)
                st.download_button(
                    label="Stáhnout data jako CSV",
                    data=csv,
                    file_name="sucho_data.csv",
                    mime="text/csv",
                )

                # Zobrazení filtrovaných dat
                with st.expander("Zobrazit filtrovaná data"):
                    st.dataframe(df)

                # Vizualizace dat
                st.header('Vizualizace dat')
                visualize_data(df)

                # Statistiky
                st.header('Statistiky sucha')
                stats_df = calculate_statistics(df)
                display_statistics(stats_df)

                # Analýza počtu dnů s různými úrovněmi sucha
                analyze_drought_days(df)

                # Další analýzy
                st.header('Další analýzy')
                
                # Top 10 nejsušších pozemků
                st.subheader('Top 10 nejsušších pozemků')
                top_10_dry = df.nlargest(10, 'mean')[['ZKOD_DPB', 'mean']]
                st.dataframe(top_10_dry)

                # Trend sucha v čase
                st.subheader('Trend sucha v čase')
                df_trend = df.melt(id_vars=['ZKOD_DPB'], 
                                   value_vars=[col for col in df.columns if col.startswith('M_')], 
                                   var_name='date', value_name='drought_level')
                df_trend['date'] = pd.to_datetime(df_trend['date'].str.split('_').str[1], format='%Y%m%d')
                df_trend = df_trend.groupby('date')['drought_level'].mean().reset_index()
                fig = px.line(df_trend, x='date', y='drought_level',
                              labels={'date': 'Datum', 'drought_level': 'Průměrná úroveň sucha'},
                              title='Trend sucha v čase')
                st.plotly_chart(fig)

                # Korelace mezi pozemky
                st.subheader('Korelace mezi pozemky')
                corr_matrix = df[[col for col in df.columns if col.startswith('M_')]].corr()
                fig = px.imshow(corr_matrix, 
                                labels=dict(color="Korelace"),
                                title="Heatmapa korelací mezi pozemky")
                st.plotly_chart(fig)
                st.write("""
                Tato heatmapa ukazuje korelace mezi úrovněmi sucha na různých pozemcích. 
                Tmavší barvy indikují silnější korelaci (podobný průběh sucha), 
                světlejší barvy indikují slabší korelaci (odlišný průběh sucha).
                """)

            else:
                st.warning("Žádná data k zobrazení. Zkuste upravit filtry.")

        with st.sidebar.expander("O aplikaci"):
            st.write("""
            Tato aplikace umožňuje prohlížení a analýzu dat o suchu v České republice.
            Vyberte filtry v postranním panelu a klikněte na tlačítko 'Filtrovat a zobrazit' pro načtení dat.
            
            Data jsou vizualizována pomocí interaktivních grafů a statistik.
            Můžete také stáhnout filtrovaná data ve formátu CSV pro další analýzu.
            
            Úroveň sucha je měřena na škále 0-5, kde:
            - 0-1: Normální stav
            - 1-2: Mírné sucho
            - 2-3: Střední sucho
            - 3-4: Vážné sucho
            - 4-5: Extrémní sucho
            """)

    except Exception as e:
        st.error("Došlo k neočekávané chybě. Detaily chyby jsou zobrazeny níže.")
        st.error(f"Typ chyby: {type(e).__name__}")
        st.error(f"Popis chyby: {str(e)}")
        st.error("Trasování chyby:")
        st.code(traceback.format_exc())
        logger.error(f"Neočekávaná chyba: {e}")
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    main()