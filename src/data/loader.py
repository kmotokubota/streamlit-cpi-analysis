"""
CPI Data Loader Module
CPIデータの読み込みとSnowflakeセッション管理
"""

import streamlit as st
import pandas as pd
from datetime import datetime, timedelta


def get_snowflake_session():
    """Snowflakeセッションを取得"""
    try:
        from snowflake.snowpark.context import get_active_session
        session = get_active_session()
        st.session_state['snowflake_available'] = True
        return session
    except Exception as e:
        st.session_state['snowflake_available'] = False
        st.error("⚠️ Snowflakeセッションを取得できません。Snowflake環境で実行してください。")
        return None


@st.cache_data(ttl=3600)
def load_cpi_categories():
    """CPI商品カテゴリを取得"""
    if not st.session_state.get('snowflake_available', False):
        return pd.DataFrame()
    
    session = get_snowflake_session()
    if session is None:
        return pd.DataFrame()
    
    try:
        query = """
        SELECT DISTINCT 
            VARIABLE,
            VARIABLE_NAME,
            SEASONALLY_ADJUSTED,
            FREQUENCY,
            UNIT,
            PRODUCT,
            BASE_TYPE
        FROM FINANCE__ECONOMICS.CYBERSYN.BUREAU_OF_LABOR_STATISTICS_PRICE_ATTRIBUTES
        WHERE REPORT = 'Consumer Price Index'
            AND VARIABLE_NAME IS NOT NULL
            AND PRODUCT IS NOT NULL
        ORDER BY PRODUCT, SEASONALLY_ADJUSTED DESC
        """
        df = session.sql(query).to_pandas()
        return df
    except Exception as e:
        st.error(f"CPIカテゴリデータの取得に失敗しました: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=3600)
def load_cpi_timeseries(variable_codes, start_date, end_date):
    """CPI時系列データを取得"""
    if not st.session_state.get('snowflake_available', False):
        return pd.DataFrame()
    
    session = get_snowflake_session()
    if session is None:
        return pd.DataFrame()
    
    try:
        # 変数コードをクエリ用に変換
        variable_list = "', '".join(variable_codes)
        
        query = f"""
        SELECT 
            ts.VARIABLE,
            ts.DATE,
            ts.VALUE,
            attr.VARIABLE_NAME,
            attr.PRODUCT,
            attr.SEASONALLY_ADJUSTED,
            attr.FREQUENCY,
            attr.UNIT,
            attr.BASE_TYPE
        FROM FINANCE__ECONOMICS.CYBERSYN.BUREAU_OF_LABOR_STATISTICS_PRICE_TIMESERIES ts
        JOIN FINANCE__ECONOMICS.CYBERSYN.BUREAU_OF_LABOR_STATISTICS_PRICE_ATTRIBUTES attr
            ON ts.VARIABLE = attr.VARIABLE
        WHERE ts.VARIABLE IN ('{variable_list}')
            AND ts.DATE >= '{start_date}'
            AND ts.DATE <= '{end_date}'
            AND ts.VALUE IS NOT NULL
        ORDER BY ts.VARIABLE, ts.DATE
        """
        df = session.sql(query).to_pandas()
        
        if not df.empty:
            df['DATE'] = pd.to_datetime(df['DATE'])
        
        return df
    except Exception as e:
        st.error(f"CPI時系列データの取得に失敗しました: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=3600)
def load_cpi_timeseries_for_contribution(start_date, end_date, frequency='Monthly'):
    """寄与度分析用のCPI時系列データを取得"""
    if not st.session_state.get('snowflake_available', False):
        return pd.DataFrame()
    
    session = get_snowflake_session()
    if session is None:
        return pd.DataFrame()
    
    try:
        # YoY計算のために開始日から12ヶ月前のデータも取得
        if isinstance(start_date, str):
            actual_start_date = datetime.strptime(start_date, '%Y-%m-%d')
        else:
            actual_start_date = start_date
        
        # 12ヶ月前からデータを取得（YoY計算のため）
        extended_start_date = actual_start_date - timedelta(days=365)
        
        query = """
        SELECT 
            ts.VARIABLE,
            ts.DATE,
            ts.VALUE,
            attr.VARIABLE_NAME,
            attr.PRODUCT,
            attr.SEASONALLY_ADJUSTED,
            attr.FREQUENCY,
            attr.UNIT
        FROM FINANCE__ECONOMICS.CYBERSYN.BUREAU_OF_LABOR_STATISTICS_PRICE_TIMESERIES ts
        JOIN FINANCE__ECONOMICS.CYBERSYN.BUREAU_OF_LABOR_STATISTICS_PRICE_ATTRIBUTES attr
            ON ts.VARIABLE = attr.VARIABLE
        WHERE attr.REPORT = 'Consumer Price Index'
            AND attr.SEASONALLY_ADJUSTED = True
            AND attr.FREQUENCY = '{}'
            AND ts.DATE >= '{}'
            AND ts.DATE <= '{}'
            AND ts.VALUE IS NOT NULL
            AND (
                attr.PRODUCT IN ('All items', 'All items less food and energy', 'Food', 'Energy', 
                               'Services less energy services', 'Commodities less food and energy commodities',
                               'Shelter', 'Transportation services', 'Food at home', 'Food away from home',
                               'New vehicles', 'Used vehicles and trucks', 'Energy services', 'Gasoline (all types)')
            )
        ORDER BY ts.VARIABLE, ts.DATE
        """.format(frequency, extended_start_date.strftime('%Y-%m-%d'), end_date)
        
        df = session.sql(query).to_pandas()
        
        if not df.empty:
            df['DATE'] = pd.to_datetime(df['DATE'])
            # 月末に正規化
            df['DATE'] = df['DATE'].dt.to_period('M').dt.end_time
        
        return df
    except Exception as e:
        st.error(f"CPI時系列データの取得に失敗しました: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=3600)
def check_contribution_data_availability():
    """寄与度データの利用可能性を確認"""
    if not st.session_state.get('snowflake_available', False):
        return False, []
    
    session = get_snowflake_session()
    if session is None:
        return False, []
    
    try:
        # ATTRIBUTESテーブルの全カラムを確認
        describe_query = """
        DESCRIBE TABLE FINANCE__ECONOMICS.CYBERSYN.BUREAU_OF_LABOR_STATISTICS_PRICE_ATTRIBUTES
        """
        
        columns_result = session.sql(describe_query).to_pandas()
        
        # カラム名を取得
        if 'NAME' in columns_result.columns:
            available_columns = columns_result['NAME'].tolist()
        elif 'name' in columns_result.columns:
            available_columns = columns_result['name'].tolist()
        elif 'column_name' in columns_result.columns:
            available_columns = columns_result['column_name'].tolist()
        else:
            # フォールバック: 最初のカラムを使用
            available_columns = columns_result.iloc[:, 0].tolist()
        
        # 寄与度関連のカラムがあるかチェック
        contribution_keywords = ['contribution', 'contrib', 'weight', 'impact', 'influence']
        contribution_columns = []
        
        for col in available_columns:
            if col and any(keyword in str(col).lower() for keyword in contribution_keywords):
                contribution_columns.append(col)
        
        return len(contribution_columns) > 0, contribution_columns
            
    except Exception as e:
        st.warning(f"寄与度データの確認中にエラーが発生しました: {str(e)}")
        return False, []