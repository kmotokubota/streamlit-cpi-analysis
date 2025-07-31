import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# Snowflakeセッション設定
try:
    from snowflake.snowpark.context import get_active_session
    session = get_active_session()
    st.session_state['snowflake_available'] = True
except:
    st.session_state['snowflake_available'] = False
    st.error("⚠️ Snowflakeセッションを取得できません。Snowflake環境で実行してください。")

# ページ設定
st.set_page_config(
    page_title="📊 CPI Analytics System",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# カスタムCSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-container {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 0.5rem 0;
    }
    .info-box {
        background-color: #e8f4fd;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
        margin: 1rem 0;
    }
    .warning-box {
        background-color: #fff3cd;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #ffc107;
        margin: 1rem 0;
    }
    .chart-title {
        font-size: 1.2rem;
        font-weight: bold;
        margin-bottom: 1.5rem;
        margin-top: 1rem;
        color: #2c3e50;
        text-align: center;
    }
    .section-title {
        font-size: 1.5rem;
        font-weight: bold;
        margin-bottom: 1rem;
        color: #2c3e50;
        text-align: left;
    }
</style>
""", unsafe_allow_html=True)

@st.cache_data(ttl=3600)
def load_cpi_categories():
    """CPI商品カテゴリを取得"""
    if not st.session_state.get('snowflake_available', False):
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
def get_major_cpi_categories():
    """主要CPIカテゴリーを定義"""
    return {
        "📈 All Items": {
            "products": ["All items"],
            "description": "総合CPI（全項目）",
            "color": "#1E88E5",  # より鮮やかな青
            "display_type": "line"
        },
        "🎯 Core CPI": {
            "products": ["All items less food and energy", "All items less food, energy, and shelter"],
            "description": "コアCPI（食品・エネルギー除く）",
            "color": "#D32F2F",  # より鮮やかな赤
            "display_type": "line"
        },
        "🍎 Food": {
            "products": ["Food", "Food at home", "Food away from home"],
            "description": "食品関連",
            "color": "#F18F01",
            "display_type": "bar"
        },
        "⚡ Energy": {
            "products": ["Energy", "Energy commodities", "Energy services", "Gasoline (all types)"],
            "description": "エネルギー関連",
            "color": "#C73E1D",
            "display_type": "bar"
        },
        "📦 Core Goods": {
            "products": ["Commodities less food and energy commodities", "New vehicles", "Used vehicles and trucks"],
            "description": "コア商品（食品・エネルギー除く）",
            "color": "#845EC2",
            "display_type": "bar"
        },
        "🏠 Core Services": {
            "products": ["Services less energy services", "Shelter", "Transportation services"],
            "description": "コアサービス（エネルギー除く）",
            "color": "#4E8397",
            "display_type": "bar"
        }
    }

@st.cache_data(ttl=3600)
def get_cpi_contribution_categories():
    """寄与度分析用の主要CPIカテゴリーとウェイトを定義"""
    # 実際のCPIバスケットの相対的ウェイト（概算）
    categories = {
        "Core Services": {
            "products": ["Services less energy services", "Shelter", "Transportation services"],
            "weight": 0.58,  # 約58%
            "color": "#4E8397",  # メインカテゴリーと同じ色
            "description": "コアサービス（住宅・交通・その他サービス）"
        },
        "Core Goods": {
            "products": ["Commodities less food and energy commodities", "New vehicles", "Used vehicles and trucks"],
            "weight": 0.20,  # 約20%
            "color": "#845EC2",  # メインカテゴリーと同じ色
            "description": "コア商品（自動車・家具・衣料等）"
        },
        "Food": {
            "products": ["Food", "Food at home", "Food away from home"],
            "weight": 0.14,  # 約14%
            "color": "#F18F01",  # メインカテゴリーと同じ色
            "description": "食品（内食・外食）"
        },
        "Energy": {
            "products": ["Energy", "Energy commodities", "Energy services", "Gasoline (all types)"],
            "weight": 0.08,  # 約8%
            "color": "#C73E1D",  # メインカテゴリーと同じ色
            "description": "エネルギー（ガソリン・電気・ガス）"
        }
    }
    return categories

@st.cache_data(ttl=3600)
def check_contribution_data_availability():
    """寄与度データの利用可能性を確認"""
    if not st.session_state.get('snowflake_available', False):
        return False, []
    
    try:
        # ATTRIBUTESテーブルの全カラムを確認
        describe_query = """
        DESCRIBE TABLE FINANCE__ECONOMICS.CYBERSYN.BUREAU_OF_LABOR_STATISTICS_PRICE_ATTRIBUTES
        """
        
        columns_result = session.sql(describe_query).to_pandas()
        
        # DESCRIBEの結果カラム名を確認（デバッグ用）
        # st.write("DEBUG - DESCRIBE結果のカラム:", columns_result.columns.tolist())
        
        # カラム名を取得（'NAME'または'name'を試す）
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

def analyze_cpi_data_with_ai_agg(df, category_name, selected_products):
    """AI_AGG関数を使用してCPIデータを分析"""
    if not st.session_state.get('snowflake_available', False):
        return "Snowflakeセッションが利用できません。"
    
    if df.empty:
        return "分析するデータがありません。"
    
    try:
        # 変数コードを取得
        variable_codes = df['VARIABLE'].unique()
        variable_list = "', '".join(variable_codes)
        
        query = f"""
        SELECT 
            AI_AGG(
                CONCAT(DATE, ': ', VARIABLE_NAME, ' = ', VALUE),
                'IMPORTANT: Please respond in Japanese only. 提供された{category_name}のCPIデータを日本語で分析し、以下の3点を150文字以内で回答してください：
                1. 最新の価格動向と傾向 2. 主要な変化要因 3. 経済や消費者への影響。
                **回答は必ず日本語のみで行ってください。英語での回答は禁止です。**
                専門的なエコノミスト視点での分析をお願いします。'
            ) AS ai_analysis
        FROM FINANCE__ECONOMICS.CYBERSYN.BUREAU_OF_LABOR_STATISTICS_PRICE_TIMESERIES
        WHERE VARIABLE IN ('{variable_list}')
            AND DATE >= DATEADD(month, -24, CURRENT_DATE())
            AND VALUE IS NOT NULL
        """
        
        result = session.sql(query).to_pandas()
        
        if not result.empty and 'AI_ANALYSIS' in result.columns:
            return result['AI_ANALYSIS'].iloc[0]
        else:
            return "AI分析結果を取得できませんでした。"
            
    except Exception as e:
        return f"AI_AGG分析でエラーが発生しました: {str(e)}"

@st.cache_data(ttl=3600)
def load_cpi_timeseries(variable_codes, start_date, end_date):
    """CPI時系列データを取得"""
    if not st.session_state.get('snowflake_available', False):
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

def calculate_inflation_metrics(df, variable_name):
    """インフレ率とその他指標を計算"""
    if df.empty:
        return {}
    
    df_sorted = df.sort_values('DATE')
    latest_value = df_sorted['VALUE'].iloc[-1]
    
    # 各期間のインフレ率計算
    metrics = {}
    
    try:
        # 月次変化率（前月比）
        if len(df_sorted) >= 2:
            prev_month_value = df_sorted['VALUE'].iloc[-2]
            metrics['monthly_change'] = ((latest_value - prev_month_value) / prev_month_value) * 100
        
        # 年次変化率（前年同月比）
        one_year_ago = df_sorted[df_sorted['DATE'] <= (df_sorted['DATE'].max() - pd.DateOffset(years=1))]
        if not one_year_ago.empty:
            year_ago_value = one_year_ago['VALUE'].iloc[-1]
            metrics['yearly_change'] = ((latest_value - year_ago_value) / year_ago_value) * 100
        
        # 3ヶ月平均変化率
        if len(df_sorted) >= 4:
            recent_3m = df_sorted['VALUE'].tail(3).mean()
            prev_3m = df_sorted['VALUE'].iloc[-6:-3].mean() if len(df_sorted) >= 6 else recent_3m
            metrics['quarterly_change'] = ((recent_3m - prev_3m) / prev_3m) * 100
        
        # ボラティリティ（標準偏差）
        if len(df_sorted) >= 12:
            monthly_changes = df_sorted['VALUE'].pct_change().dropna()
            metrics['volatility'] = monthly_changes.std() * 100
        
        metrics['current_level'] = latest_value
        metrics['latest_date'] = df_sorted['DATE'].max()
        
    except Exception as e:
        st.warning(f"{variable_name}の指標計算でエラーが発生しました: {str(e)}")
    
    return metrics


def get_periods_for_frequency(frequency):
    """頻度に応じた前年同期比計算の期間数を返す"""
    frequency_periods = {
        'Monthly': 12,       # 12ヶ月前
        'Quarterly': 4,      # 4四半期前
        'Semi-annual': 2,    # 2半期前
        'Annual': 1          # 1年前
    }
    return frequency_periods.get(frequency, 12)  # デフォルトは Monthly

def calculate_yoy_monthly_data(df):
    """毎月のYoY%データを計算"""
    if df.empty:
        return pd.DataFrame()
    
    df_sorted = df.sort_values('DATE').copy()
    
    # 前年同月比を計算
    df_sorted['YoY_Change'] = df_sorted['VALUE'].pct_change(periods=12) * 100
    
    # 前月比を計算
    df_sorted['MoM_Change'] = df_sorted['VALUE'].pct_change() * 100
    
    # 年と月を分離
    df_sorted['Year'] = df_sorted['DATE'].dt.year
    df_sorted['Month'] = df_sorted['DATE'].dt.month
    df_sorted['Month_Name'] = df_sorted['DATE'].dt.strftime('%b')
    
    return df_sorted

def create_stacked_histogram(df, selected_products, chart_type="yoy"):
    """積み上げヒストグラム作成（商品別、インフレ率比較チャート形式）"""
    if df.empty or not selected_products:
        return go.Figure()
    
    y_title = "前年同月比 (%)" if chart_type == "yoy" else "前月比 (%)"
    
    # 各商品の最新データを取得
    latest_data = {}
    for product in selected_products:
        product_data = df[df['PRODUCT'] == product]
        if not product_data.empty:
            calc_data = calculate_yoy_monthly_data(product_data)
            if not calc_data.empty:
                # 最新のデータを取得
                latest_row = calc_data.iloc[-1]
                if chart_type == "yoy":
                    value = latest_row['YoY_Change']
                else:
                    value = latest_row['MoM_Change']
                latest_data[product] = value
    
    if not latest_data:
        return go.Figure()
    
    # データを正負で分離
    products = list(latest_data.keys())
    values = list(latest_data.values())
    
    positive_values = [v if v >= 0 else 0 for v in values]
    negative_values = [v if v < 0 else 0 for v in values]
    
    fig = go.Figure()
    
    # 正の値のバー（青系）
    if any(v > 0 for v in positive_values):
        fig.add_trace(go.Bar(
            name='正の変化率',
            x=products,
            y=positive_values,
            marker_color='#1f77b4',
            opacity=0.85,
            hovertemplate=f'<b>%{{x}}</b><br>{y_title}: %{{y:.2f}}%<extra></extra>'
        ))
    
    # 負の値のバー（赤系）
    if any(v < 0 for v in negative_values):
        fig.add_trace(go.Bar(
            name='負の変化率',
            x=products,
            y=negative_values,
            marker_color='#d62728',
            opacity=0.85,
            hovertemplate=f'<b>%{{x}}</b><br>{y_title}: %{{y:.2f}}%<extra></extra>'
        ))
    
    # Y軸の範囲を動的に設定（詳細な計算）
    if values:
        # 有効な数値データのみを使用
        valid_data = [x for x in values if not pd.isna(x) and abs(x) < 1000]
        
        if valid_data:
            y_min = min(valid_data)
            y_max = max(valid_data)
            y_range_size = y_max - y_min
            
            # 適切なマージンを追加
            if y_range_size > 0.1:  # 最小範囲チェック
                margin = max(y_range_size * 0.15, 0.5)  # 最小マージン確保
                y_range = [y_min - margin, y_max + margin]
            else:
                # データ範囲が小さい場合
                center = (y_min + y_max) / 2
                y_range = [center - 1, center + 1]
            
            # ゼロラインを含むように調整
            if y_range[0] > 0.1:
                y_range[0] = min(y_range[0], -0.2)
            if y_range[1] < -0.1:
                y_range[1] = max(y_range[1], 0.2)
        else:
            y_range = [-1, 5]

    # Y軸の範囲を動的に設定（詳細な計算）
    if values:
        # 有効な数値データのみを使用
        valid_data = [x for x in values if not pd.isna(x) and abs(x) < 1000]
        
        if valid_data:
            y_min = min(valid_data)
            y_max = max(valid_data)
            y_range_size = y_max - y_min
            
            # 適切なマージンを追加
            if y_range_size > 0.1:  # 最小範囲チェック
                margin = max(y_range_size * 0.15, 0.5)  # 最小マージン確保
                y_range = [y_min - margin, y_max + margin]
            else:
                # データ範囲が小さい場合
                center = (y_min + y_max) / 2
                y_range = [center - 1, center + 1]
            
            # ゼロラインを含むように調整
            if y_range[0] > 0.1:
                y_range[0] = min(y_range[0], -0.2)
            if y_range[1] < -0.1:
                y_range[1] = max(y_range[1], 0.2)
        else:
            y_range = [-1, 5]
    else:
        y_range = [-1, 5]

    # レイアウト設定
    fig.update_layout(
        title={
            'text': f'📊 {y_title}積み上げ分析',
            'x': 0,
            'font': {'size': 18, 'family': 'Arial, sans-serif'}
        },
        xaxis_title='商品・サービス',
        yaxis_title=y_title,
        barmode='stack',
        height=600,
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
        plot_bgcolor='white',
        paper_bgcolor='white'
    )
    
    # Y軸範囲を確実に設定
    fig.update_yaxes(range=y_range)
    
    # ゼロラインの追加
    fig.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.7, line_width=1)
    
    # グリッドの設定
    fig.update_xaxes(showgrid=True, gridwidth=0.5, gridcolor='lightgray')
    fig.update_yaxes(showgrid=True, gridwidth=0.5, gridcolor='lightgray')
    
    # x軸ラベルの角度調整（商品名が長い場合）
    fig.update_xaxes(tickangle=-45)
    
    return fig

def create_major_category_chart(df, selected_categories, chart_type="yoy"):
    """主要カテゴリー用のチャート作成（バー+ライン）"""
    if df.empty:
        return go.Figure()
    
    # y_titleを初期化
    if chart_type == "yoy":
        y_title = "前年同月比 (%)"
    else:
        y_title = "前月比 (%)"
    
    fig = go.Figure()
    major_categories = get_major_cpi_categories()
    
    # Y軸の範囲を動的に設定するための変数
    all_y_values = []
    
    for category in selected_categories:
        if category in major_categories:
            category_info = major_categories[category]
            category_products = category_info['products']
            display_type = category_info.get('display_type', 'bar')
            color = category_info['color']
            
            # カテゴリーに該当するデータを抽出
            category_data = df[df['PRODUCT'].isin(category_products)]
            
            if not category_data.empty:
                # 最も代表的な商品を選択
                if len(category_products) == 1:
                    main_product = category_products[0]
                else:
                    # 最もデータが多い商品を選択、見つからない場合は最初の商品を使用
                    product_counts = category_data.groupby('PRODUCT').size()
                    if not product_counts.empty:
                        main_product = product_counts.idxmax()
                    else:
                        main_product = category_products[0]
                
                product_data = category_data[category_data['PRODUCT'] == main_product].copy()
                
                if not product_data.empty:
                    # YoYデータを計算
                    yoy_data = calculate_yoy_monthly_data(product_data)
                    
                    if chart_type == "yoy":
                        y_values = yoy_data['YoY_Change'].dropna()
                    else:
                        y_values = yoy_data['MoM_Change'].dropna()
                    
                    if not y_values.empty:
                        category_name = category.split(' ', 1)[1] if ' ' in category else category
                        
                        # Y軸範囲計算用にデータを保存
                        all_y_values.extend(y_values.tolist())
                        
                        if display_type == "bar":
                            # バー表示（Food, Energy, Core Goods, Core Services）
                            fig.add_trace(go.Bar(
                                x=yoy_data['DATE'].iloc[-len(y_values):],
                                y=y_values,
                                name=category_name,
                                marker_color=color,
                                opacity=0.85,
                                hovertemplate=(
                                    f'<b>{category_name}</b><br>'
                                    '日付: %{x}<br>'
                                    f'{y_title}: %{{y:.2f}}%<br>'
                                    '<extra></extra>'
                                )
                            ))
                        else:
                            # ライン表示（All Items, Core CPI）
                            fig.add_trace(go.Scatter(
                                x=yoy_data['DATE'].iloc[-len(y_values):],
                                y=y_values,
                                mode='lines+markers',
                                name=category_name,
                                line=dict(color=color, width=5),  # より太いライン
                                marker=dict(size=10, color=color, symbol='circle'),  # より大きなマーカー
                                hovertemplate=(
                                    f'<b>{category_name}</b><br>'
                                    '日付: %{x}<br>'
                                    f'{y_title}: %{{y:.2f}}%<br>'
                                    '<extra></extra>'
                                )
                            ))
    
    # Y軸の範囲を動的に設定（詳細な計算）
    if all_y_values:
        # 有効な数値データのみを使用
        valid_data = [x for x in all_y_values if not pd.isna(x) and abs(x) < 1000]
        
        if valid_data:
            y_min = min(valid_data)
            y_max = max(valid_data)
            y_range_size = y_max - y_min
            
            # 適切なマージンを追加
            if y_range_size > 0.1:  # 最小範囲チェック
                margin = max(y_range_size * 0.15, 0.5)  # 最小マージン確保
                y_range = [y_min - margin, y_max + margin]
            else:
                # データ範囲が小さい場合
                center = (y_min + y_max) / 2
                y_range = [center - 1, center + 1]
            
            # ゼロラインを含むように調整
            if y_range[0] > 0.1:
                y_range[0] = min(y_range[0], -0.2)
            if y_range[1] < -0.1:
                y_range[1] = max(y_range[1], 0.2)
        else:
            y_range = [-1, 5]
    else:
        y_range = [-1, 5]
    
    fig.update_layout(
        title={
            'text': f'📊 主要カテゴリー CPI {y_title}推移（積み上げ表示）',
            'x': 0,
            'font': {'size': 18, 'family': 'Arial, sans-serif'}
        },
        xaxis_title='期間',
        yaxis_title=y_title,
        barmode='stack',
        height=600,
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
        hovermode='x unified',
        plot_bgcolor='white',
        paper_bgcolor='white'
    )
    
    # Y軸範囲を確実に設定
    fig.update_yaxes(range=y_range)
    
    # ゼロラインの追加
    fig.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.7, line_width=1)
    
    # グリッドの設定
    fig.update_xaxes(showgrid=True, gridwidth=0.5, gridcolor='lightgray')
    fig.update_yaxes(showgrid=True, gridwidth=0.5, gridcolor='lightgray')
    
    return fig

def create_monthly_yoy_breakdown_chart(df, selected_products):
    """月別YoY%内訳表示（エコノミスト向け詳細分析）"""
    if df.empty:
        return go.Figure()
    
    # サブプロットを作成（商品ごと）
    fig = make_subplots(
        rows=len(selected_products), 
        cols=1,
        subplot_titles=[f"{product} - 月別前年同月比" for product in selected_products],
        vertical_spacing=0.08,
        shared_xaxes=True
    )
    
    colors = ['#3498db', '#e74c3c', '#2ecc71', '#f39c12', '#9b59b6']
    
    for i, product in enumerate(selected_products):
        product_data = df[df['PRODUCT'] == product].copy()
        
        if not product_data.empty:
            yoy_data = calculate_yoy_monthly_data(product_data)
            
            # 最近24ヶ月のデータを取得
            recent_data = yoy_data.tail(24)
            
            if not recent_data.empty:
                fig.add_trace(
                    go.Bar(
                        x=recent_data['DATE'],
                        y=recent_data['YoY_Change'],
                        name=f"{product}",
                        marker_color=colors[i % len(colors)],
                        showlegend=(i == 0),
                        hovertemplate=(
                            f'<b>{product}</b><br>'
                            '日付: %{x}<br>'
                            '前年同月比: %{y:.2f}%<br>'
                            '<extra></extra>'
                        )
                    ),
                    row=i+1, col=1
                )
                
                # 各サブプロットにゼロライン追加
                fig.add_hline(
                    y=0, line_dash="dash", line_color="gray", 
                    opacity=0.5, row=i+1, col=1
                )
    
    fig.update_layout(
        title={
            'text': '📈 商品別月次前年同月比詳細分析',
            'x': 0,
            'font': {'size': 18, 'family': 'Arial, sans-serif'}
        },
        height=400 * len(selected_products),
        showlegend=False,
        plot_bgcolor='white',
        paper_bgcolor='white'
    )
    
    # 全サブプロットのx軸とy軸を設定
    for i in range(len(selected_products)):
        fig.update_xaxes(showgrid=True, gridwidth=0.5, gridcolor='lightgray', row=i+1, col=1)
        fig.update_yaxes(
            showgrid=True, gridwidth=0.5, gridcolor='lightgray',
            title_text="前年同月比 (%)", row=i+1, col=1
        )
    
    return fig

def generate_ai_analysis(selected_products, inflation_data, ai_model="llama3.1-70b"):
    """AI分析を生成（専門的な経済分析）"""
    if not st.session_state.get('snowflake_available', False):
        return "Snowflakeセッションが利用できないため、AI分析を実行できません。"
    
    try:
        # 分析データの要約作成
        analysis_summary = []
        for product, metrics in inflation_data.items():
            if metrics:
                yearly_change = metrics.get('yearly_change', 0)
                monthly_change = metrics.get('monthly_change', 0)
                current_level = metrics.get('current_level', 0)
                
                analysis_summary.append(f"""
                【{product}】
                - 現在水準: {current_level:.1f}
                - 前年同月比: {yearly_change:+.2f}%
                - 前月比: {monthly_change:+.2f}%
                """)
        
        summary_text = "\n".join(analysis_summary)
        
        prompt = f"""
        消費者物価指数（CPI）の専門的な経済分析を、エコノミストの視点で実施してください。
        プロフェッショナルな分析スタイルで回答してください。

        【分析対象商品・サービス】
        {', '.join(selected_products)}

        【最新のCPIデータ】
        {summary_text}

        【分析要求】
        1. 各商品・サービスの価格動向の詳細分析
        2. 月次変動の主要ドライバー特定
        3. インフレーション圧力の根本要因評価
        4. 連邦準備制度への政策的含意
        5. 今後3-6ヶ月の見通しと主要リスクファクター

        【出力形式】
        - エコノミスト向けの高度で専門的な分析
        - 具体的な数値とトレンドを基にした根拠
        - 金融政策への具体的な提言
        - 簡潔で実用的な内容（600文字以内）
        - 専門用語を適切に使用

        参考スタイル: "The X.X% month over month increase was caused by..."のような具体的分析
        """
        
        ai_query = f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE('{ai_model}', '{prompt}') as analysis
        """
        
        result = session.sql(ai_query).to_pandas()
        return result['ANALYSIS'].iloc[0] if not result.empty else "AI分析の生成に失敗しました。"
        
    except Exception as e:
        return f"AI分析でエラーが発生しました: {str(e)}"

def create_cpi_comparison_chart(df, selected_products):
    """CPI比較チャート（従来のライン表示）"""
    if df.empty:
        return go.Figure()
    
    fig = go.Figure()
    
    colors = px.colors.qualitative.Set1
    
    for i, product in enumerate(selected_products):
        product_data = df[df['PRODUCT'] == product].sort_values('DATE')
        
        if not product_data.empty:
            # YoY%データを計算
            yoy_data = calculate_yoy_monthly_data(product_data)
            if not yoy_data.empty:
                # 最新24ヶ月のデータに限定
                recent_data = yoy_data.tail(24)
                
                # ライン表示に変更
                fig.add_trace(go.Scatter(
                    x=recent_data['DATE'],
                    y=recent_data['YoY_Change'],
                    mode='lines+markers',
                    name=product,
                    line=dict(color=colors[i % len(colors)], width=3),
                    marker=dict(size=6),
                    hovertemplate=f'<b>{product}</b><br>日付: %{{x}}<br>前年同月比: %{{y:.2f}}%<extra></extra>'
                ))
    
    fig.update_layout(
        title={
            'text': '📈 CPI前年同月比推移（ライン表示）',
            'x': 0,
            'font': {'size': 18, 'family': 'Arial, sans-serif'}
        },
        xaxis_title='期間',
        yaxis_title='前年同月比 (%)',
        hovermode='x unified',
        height=600,
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
        plot_bgcolor='white',
        paper_bgcolor='white'
    )
    
    # ゼロラインの追加
    fig.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.7, line_width=1)
    
    # グリッドの設定
    fig.update_xaxes(showgrid=True, gridwidth=0.5, gridcolor='lightgray')
    fig.update_yaxes(showgrid=True, gridwidth=0.5, gridcolor='lightgray')
    
    return fig

def add_all_items_baseline_to_chart(fig, df):
    """既存のチャートにAll Items YoY%ベースラインを追加"""
    if df.empty:
        return fig
    
    # All Itemsデータを抽出
    all_items_data = df[df['PRODUCT'] == 'All items'].copy()
    
    if all_items_data.empty:
        return fig
    
    all_items_data = all_items_data.sort_values('DATE')
    
    # YoYデータを計算
    yoy_data = calculate_yoy_monthly_data(all_items_data)
    y_values = yoy_data['YoY_Change'].dropna()
    
    if y_values.empty:
        return fig
    
    # All Items YoY%ラインを追加（太いラインで強調）
    fig.add_trace(go.Scatter(
        x=yoy_data['DATE'].iloc[-len(y_values):],
        y=y_values,
        mode='lines',
        name='📈 All Items CPI (ベースライン)',
        line=dict(color='#2E86AB', width=4, dash='solid'),
        hovertemplate='<b>All Items CPI (ベースライン)</b><br>日付: %{x}<br>前年同月比: %{y:.2f}%<extra></extra>',
        yaxis='y'
    ))
    
    return fig

def create_inflation_rate_chart(inflation_data):
    """インフレ率比較チャート作成"""
    if not inflation_data:
        return go.Figure()
    
    products = []
    yearly_changes = []
    monthly_changes = []
    
    for product, metrics in inflation_data.items():
        if metrics and 'yearly_change' in metrics:
            products.append(product)
            yearly_changes.append(metrics.get('yearly_change', 0))
            monthly_changes.append(metrics.get('monthly_change', 0))
    
    if not products:
        return go.Figure()
    
    fig = go.Figure()
    
    # 前年同月比
    fig.add_trace(go.Bar(
        name='前年同月比',
        x=products,
        y=yearly_changes,
        marker_color='lightblue',
        hovertemplate='<b>%{x}</b><br>前年同月比: %{y:.2f}%<extra></extra>'
    ))
    
    # 前月比
    fig.add_trace(go.Bar(
        name='前月比',
        x=products,
        y=monthly_changes,
        marker_color='lightcoral',
        hovertemplate='<b>%{x}</b><br>前月比: %{y:.2f}%<extra></extra>'
    ))
    
    fig.update_layout(
        title={
            'text': '📊 インフレ率比較（前年同月比 vs 前月比）',
            'x': 0,
            'font': {'size': 18, 'family': 'Arial, sans-serif'}
        },
        xaxis_title='商品・サービス',
        yaxis_title='変化率 (%)',
        barmode='stack',
        height=600,
        showlegend=True
    )
    
    # ゼロラインの追加
    fig.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.5)
    
    return fig

@st.cache_data(ttl=3600)
def check_contribution_data_availability():
    """寄与度データの利用可能性を確認"""
    if not st.session_state.get('snowflake_available', False):
        return False, []
    
    try:
        # ATTRIBUTESテーブルの全カラムを確認
        describe_query = """
        DESCRIBE TABLE FINANCE__ECONOMICS.CYBERSYN.BUREAU_OF_LABOR_STATISTICS_PRICE_ATTRIBUTES
        """
        
        columns_result = session.sql(describe_query).to_pandas()
        
        # DESCRIBEの結果カラム名を確認（デバッグ用）
        # st.write("DEBUG - DESCRIBE結果のカラム:", columns_result.columns.tolist())
        
        # カラム名を取得（'NAME'または'name'を試す）
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
        
        # サンプルデータで寄与度データの存在を確認
        if contribution_columns:
            sample_query = f"""
            SELECT {', '.join(contribution_columns)}, COUNT(*) as data_count
            FROM FINANCE__ECONOMICS.CYBERSYN.BUREAU_OF_LABOR_STATISTICS_PRICE_ATTRIBUTES
            WHERE REPORT = 'Consumer Price Index'
            GROUP BY {', '.join(contribution_columns)}
            LIMIT 10
            """
            
            sample_result = session.sql(sample_query).to_pandas()
            
            return True, contribution_columns
        else:
            return False, []
            
    except Exception as e:
        st.warning(f"寄与度データの確認中にエラーが発生しました: {str(e)}")
        return False, []

def create_individual_products_chart(df, selected_products, chart_type="yoy"):
    """個別商品用のチャート作成（主要カテゴリーと同じ形式）"""
    if df.empty or not selected_products:
        return go.Figure()
    
    # y_titleを初期化
    if chart_type == "yoy":
        y_title = "前年同月比 (%)"
    else:
        y_title = "前月比 (%)"
    
    fig = go.Figure()
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b']
    
    for i, product in enumerate(selected_products):
        product_data = df[df['PRODUCT'] == product].copy()
        
        if not product_data.empty:
            # YoYデータを計算
            yoy_data = calculate_yoy_monthly_data(product_data)
            
            if chart_type == "yoy":
                y_values = yoy_data['YoY_Change'].dropna()
            else:
                y_values = yoy_data['MoM_Change'].dropna()
            
            if not y_values.empty:
                # 最新24ヶ月のデータに限定
                recent_data = yoy_data.tail(24)
                
                if chart_type == "yoy":
                    y_recent = recent_data['YoY_Change'].dropna()
                else:
                    y_recent = recent_data['MoM_Change'].dropna()
                
                if not y_recent.empty:
                    # 短縮された商品名
                    short_name = product.replace("All items less ", "").replace("All items", "All Items")
                    
                    fig.add_trace(go.Bar(
                        x=recent_data['DATE'].iloc[-len(y_recent):],
                        y=y_recent,
                        name=short_name,
                        marker_color=colors[i % len(colors)],
                        opacity=0.85,
                        hovertemplate=(
                            f'<b>{short_name}</b><br>'
                            '日付: %{x}<br>'
                            f'{y_title}: %{{y:.2f}}%<br>'
                            '<extra></extra>'
                        )
                    ))
    
    fig.update_layout(
        title={
            'text': f'📊 CPI {y_title}推移（積み上げ表示）',
            'x': 0,
            'font': {'size': 18, 'family': 'Arial, sans-serif'}
        },
        xaxis_title='期間',
        yaxis_title=y_title,
        barmode='stack',
        height=600,
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
        hovermode='x unified',
        plot_bgcolor='white',
        paper_bgcolor='white'
    )
    
    # ゼロラインの追加
    fig.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.7, line_width=1)
    
    # グリッドの設定
    fig.update_xaxes(showgrid=True, gridwidth=0.5, gridcolor='lightgray')
    fig.update_yaxes(showgrid=True, gridwidth=0.5, gridcolor='lightgray')
    
    return fig

def calculate_contribution_data_for_categories(df, selected_categories=None, user_start_date=None):
    """各カテゴリーの寄与度を計算（主要カテゴリー用、選択カテゴリー対応）"""
    if df.empty:
        return pd.DataFrame()
    
    # All Itemsデータを取得
    all_items_data = df[df['PRODUCT'] == 'All items'].copy()
    if all_items_data.empty:
        return pd.DataFrame()
    
    # 前年同月比計算
    all_items_data = all_items_data.sort_values('DATE')
    # データの頻度を推定（Monthly がデフォルト）
    if not df.empty and 'FREQUENCY' in df.columns:
        frequency = df['FREQUENCY'].iloc[0]
    else:
        frequency = 'Monthly'
    
    periods = get_periods_for_frequency(frequency)
    all_items_data['YoY_Change'] = all_items_data['VALUE'].pct_change(periods=periods) * 100
    
    # Core CPIデータを取得（選択されている場合のみ）
    show_core_cpi = selected_categories is None or '🎯 Core CPI' in selected_categories
    core_cpi_data = pd.DataFrame()  # 初期化
    if show_core_cpi:
        core_cpi_data = df[df['PRODUCT'] == 'All items less food and energy'].copy()
        if not core_cpi_data.empty:
            core_cpi_data = core_cpi_data.sort_values('DATE')
            core_cpi_data['YoY_Change'] = core_cpi_data['VALUE'].pct_change(periods=periods) * 100
    
    # カテゴリー定義（選択されたカテゴリーのみ）
    all_categories = get_cpi_contribution_categories()
    
    # 寄与度計算は常に全カテゴリーで実行（表示フィルタリングは後で行う）
    categories = all_categories
    
    contribution_data = []
    
    for date in all_items_data['DATE'].unique():
        if pd.isna(date):
            continue
            
        date_data = df[df['DATE'] == date]
        all_items_yoy = all_items_data[all_items_data['DATE'] == date]['YoY_Change'].iloc[0] if len(all_items_data[all_items_data['DATE'] == date]) > 0 else 0
        
        if pd.isna(all_items_yoy):
            continue
            
        for category_name, category_info in categories.items():
            # カテゴリー内の商品データを取得
            category_data = date_data[date_data['PRODUCT'].isin(category_info['products'])]
            
            if not category_data.empty:
                # 代表的な商品の変化率を使用
                if category_name == "Core Services" and len(category_data[category_data['PRODUCT'] == 'Services less energy services']) > 0:
                    representative_product = category_data[category_data['PRODUCT'] == 'Services less energy services'].iloc[0]
                elif category_name == "Core Goods" and len(category_data[category_data['PRODUCT'] == 'Commodities less food and energy commodities']) > 0:
                    representative_product = category_data[category_data['PRODUCT'] == 'Commodities less food and energy commodities'].iloc[0]
                elif category_name == "Food" and len(category_data[category_data['PRODUCT'] == 'Food']) > 0:
                    representative_product = category_data[category_data['PRODUCT'] == 'Food'].iloc[0]
                elif category_name == "Energy" and len(category_data[category_data['PRODUCT'] == 'Energy']) > 0:
                    representative_product = category_data[category_data['PRODUCT'] == 'Energy'].iloc[0]
                else:
                    continue
                
                # 前年同月比計算（個別に）
                product_historical = df[(df['PRODUCT'] == representative_product['PRODUCT']) & 
                                      (df['DATE'] <= date)].sort_values('DATE')
                
                required_periods = periods + 1  # 前年同期 + 現在のデータが必要
                if len(product_historical) >= required_periods:
                    current_value = product_historical.iloc[-1]['VALUE']
                    year_ago_value = product_historical.iloc[-1-periods]['VALUE']
                    category_yoy = ((current_value / year_ago_value) - 1) * 100
                    
                    # 寄与度計算（ウェイト × 変化率）
                    contribution = category_info['weight'] * category_yoy
                    
                    # Core CPI YoYを追加（選択されている場合のみ）
                    core_cpi_yoy = None
                    if show_core_cpi and not core_cpi_data.empty:
                        core_date_data = core_cpi_data[core_cpi_data['DATE'] == date]
                        if not core_date_data.empty:
                            core_cpi_yoy = core_date_data['YoY_Change'].iloc[0]
                    
                    # ユーザー指定の開始日以降のデータのみを追加
                    if user_start_date is None or date >= pd.to_datetime(user_start_date):
                        contribution_data.append({
                            'DATE': date,
                            'Category': category_name,
                            'Contribution': contribution,
                            'Weight': category_info['weight'],
                            'YoY_Change': category_yoy,
                            'Color': category_info['color'],
                            'All_Items_YoY': all_items_yoy,
                            'Core_CPI_YoY': core_cpi_yoy
                        })
    
    return pd.DataFrame(contribution_data)

def create_bloomberg_contribution_chart_integrated(contribution_df, selected_categories=None):
    """前月比積み上げヒストグラムと同じスタイルの寄与度チャート（All Items/Core CPIライン付き、動的Y軸調整、選択カテゴリー対応）"""
    if contribution_df.empty:
        return go.Figure()
    
    # データを月ごとにピボット
    pivot_df = contribution_df.pivot(index='DATE', columns='Category', values='Contribution').fillna(0)
    
    # チャート作成
    fig = go.Figure()
    
    # カテゴリー順序とカラー（選択されたカテゴリーのみ）
    categories = get_cpi_contribution_categories()
    
    # 選択されたカテゴリーに基づいてフィルタリング
    if selected_categories:
        category_mapping = {
            '⚡ Energy': 'Energy',
            '🍎 Food': 'Food', 
            '📦 Core Goods': 'Core Goods',
            '🏠 Core Services': 'Core Services'
        }
        
        filtered_categories = []
        for selected_cat in selected_categories:
            if selected_cat in category_mapping:
                filtered_categories.append(category_mapping[selected_cat])
        
        category_order = filtered_categories if filtered_categories else []
    else:
        category_order = ['Energy', 'Food', 'Core Goods', 'Core Services']
    
    # 積み上げ棒グラフの作成
    for category in category_order:
        if category in pivot_df.columns:
            color = categories[category]['color']
            
            fig.add_trace(go.Bar(
                name=category,
                x=pivot_df.index,
                y=pivot_df[category],
                marker_color=color,
                opacity=0.85,
                hovertemplate=f'<b>{category}</b><br>' +
                             'Date: %{x}<br>' +
                             'Contribution: %{y:.2f}pp<br>' +
                             '<extra></extra>'
            ))
    
    # All Items CPIライン（選択されている場合のみ表示）
    show_all_items = selected_categories is None or '📈 All Items' in selected_categories
    if show_all_items and not contribution_df.empty and 'All_Items_YoY' in contribution_df.columns:
        all_items_line = contribution_df.groupby('DATE')['All_Items_YoY'].first().dropna()
        if not all_items_line.empty:
            fig.add_trace(go.Scatter(
                name='All Items CPI (YoY)',
                x=all_items_line.index,
                y=all_items_line.values,
                mode='lines+markers',
                line=dict(color='#1E88E5', width=2),
                marker=dict(size=8, color='#1E88E5'),
                hovertemplate='<b>All Items CPI</b><br>' +
                             'Date: %{x}<br>' +
                             'YoY: %{y:.2f}%<br>' +
                             '<extra></extra>'
            ))
    
    # Core CPIライン（選択されている場合のみ表示）
    show_core_cpi = selected_categories is None or '🎯 Core CPI' in selected_categories
    if show_core_cpi and not contribution_df.empty and 'Core_CPI_YoY' in contribution_df.columns:
        core_cpi_line = contribution_df.groupby('DATE')['Core_CPI_YoY'].first().dropna()
        if not core_cpi_line.empty:
            fig.add_trace(go.Scatter(
                name='Core CPI (YoY)',
                x=core_cpi_line.index,
                y=core_cpi_line.values,
                mode='lines+markers',
                line=dict(color='#D32F2F', width=2),
                marker=dict(size=8, color='#D32F2F'),
                hovertemplate='<b>Core CPI</b><br>' +
                             'Date: %{x}<br>' +
                             'YoY: %{y:.2f}%<br>' +
                             '<extra></extra>'
            ))
    
    # Y軸範囲計算：積み上げデータとラインデータを分離
    bar_data = []
    line_data = []
    
    # 積み上げ棒グラフのデータを収集
    for category in category_order:
        if category in pivot_df.columns:
            category_data = pivot_df[category].dropna()
            bar_data.extend(category_data.tolist())
    
    # ラインチャートのデータを条件に応じて収集
    if show_all_items and not contribution_df.empty and 'All_Items_YoY' in contribution_df.columns:
        all_items_line = contribution_df.groupby('DATE')['All_Items_YoY'].first().dropna()
        if not all_items_line.empty:
            line_data.extend(all_items_line.tolist())
    
    if show_core_cpi and not contribution_df.empty and 'Core_CPI_YoY' in contribution_df.columns:
        core_cpi_line = contribution_df.groupby('DATE')['Core_CPI_YoY'].first().dropna()
        if not core_cpi_line.empty:
            line_data.extend(core_cpi_line.tolist())
    
    # 実際に表示されるデータを結合
    displayed_data = bar_data + line_data
    
    
    # Y軸範囲を表示されるデータに基づいて設定
    if displayed_data and len(displayed_data) > 0:
        # 有効な数値データのみを使用
        valid_data = [x for x in displayed_data if not pd.isna(x) and abs(x) < 1000]
        
        if valid_data:
            y_min = min(valid_data)
            y_max = max(valid_data)
            y_range_size = y_max - y_min
            
            # 適切なマージンを追加
            if y_range_size > 0.1:  # 最小範囲チェック
                margin = max(y_range_size * 0.15, 0.5)  # 最小マージン確保
                y_range = [y_min - margin, y_max + margin]
            else:
                # データ範囲が小さい場合
                center = (y_min + y_max) / 2
                y_range = [center - 1, center + 1]
            
            # ゼロラインを含むように調整
            if y_range[0] > 0.1:
                y_range[0] = min(y_range[0], -0.2)
            if y_range[1] < -0.1:
                y_range[1] = max(y_range[1], 0.2)
        else:
            y_range = [-1, 3]
    else:
        # データがない場合のデフォルト範囲
        y_range = [-1, 3]
    
    # レイアウト設定（確実なY軸範囲適用）
    fig.update_layout(
        title={
            'text': '📊 主要カテゴリー CPI 寄与度分析（積み上げ表示）',
            'x': 0,
            'font': {'size': 18, 'family': 'Arial, sans-serif'}
        },
        xaxis_title='期間',
        yaxis_title='前年同月比 / 寄与度 (%)',
        barmode='stack',
        height=600,
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
        hovermode='x unified',
        plot_bgcolor='white',
        paper_bgcolor='white'
    )
    
    # Y軸範囲を確実に設定
    fig.update_yaxes(range=y_range)
    
    # ゼロラインの追加（前月比積み上げヒストグラムと同じ）
    fig.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.7, line_width=1)
    
    # FRBターゲットライン（2%がY軸範囲内にある場合のみ表示）
    if len(y_range) >= 2 and y_range[0] <= 2 <= y_range[1]:
        fig.add_hline(y=2, line_color="gray", line_width=0.5, line_dash="dash", 
                      opacity=0.5)
    
    # グリッドの設定（前月比積み上げヒストグラムと同じ）
    fig.update_xaxes(showgrid=True, gridwidth=0.5, gridcolor='lightgray')
    fig.update_yaxes(showgrid=True, gridwidth=0.5, gridcolor='lightgray')
    
    return fig

@st.cache_data(ttl=3600)
def load_cpi_timeseries_for_contribution(start_date, end_date, frequency='Monthly'):
    """寄与度分析用のCPI時系列データを取得"""
    if not st.session_state.get('snowflake_available', False):
        return pd.DataFrame()
    
    try:
        # YoY計算のために開始日から12ヶ月前のデータも取得
        from datetime import datetime, timedelta
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

def main():
    """メイン関数"""
    
    if not st.session_state.get('snowflake_available', False):
        st.stop()
    
    # メインタイトル
    st.markdown('<div class="main-header">📊 CPI分析ダッシュボード</div>', unsafe_allow_html=True)

    # データ説明
    st.markdown("""
    <div class="info-box">
    <b>🏛️ データソース:</b> U.S. Bureau of Labor Statistics (BLS) Consumer Price Index via Cybersyn on Snowflake<br>
    <b>📊 分析対象:</b> 米国消費者物価指数 (CPI) の詳細カテゴリー別動向分析<br>
    <b>🎯 対象ユーザー:</b> マーケットエコノミスト、金融アナリスト、政策立案者
    </div>
    """, unsafe_allow_html=True)

    # サイドバー設定
    st.sidebar.header(" 分析設定")
    
    # 日付範囲選択
    st.sidebar.subheader("📅 分析期間")
    col1, col2 = st.sidebar.columns(2)
    with col1:
        start_date = st.date_input(
            "開始日",
            value=datetime.now() - timedelta(days=365*3),
            max_value=datetime.now()
        )
    with col2:
        end_date = st.date_input(
            "終了日",
            value=datetime.now(),
            max_value=datetime.now()
        )
    
    # Snowflakeセッション確認
    if not st.session_state.get('snowflake_available', False):
        st.error("❌ Snowflakeセッションが利用できません。")
        st.stop()
    
    # CPI商品カテゴリ取得
    categories_df = load_cpi_categories()
    if categories_df.empty:
        st.error("❌ CPIカテゴリデータの取得に失敗しました。")
        st.stop()
    
    # 商品選択UI
    st.sidebar.subheader("🛍️ 分析対象商品・サービス")
    
    # 季節調整の選択
    seasonal_adjustment = st.sidebar.radio(
        "季節調整",
        ["季節調整済み", "季節調整なし", "両方"],
        index=0
    )
    
    # 頻度選択
    frequency_options = sorted(categories_df['FREQUENCY'].unique())
    selected_frequency = st.sidebar.selectbox(
        "データ頻度",
        frequency_options,
        index=frequency_options.index('Monthly') if 'Monthly' in frequency_options else 0,
        help="データ頻度を選択。寄与度分析も含めてすべての分析に適用されます。"
    )
    
    # 季節調整フィルタ
    if seasonal_adjustment == "季節調整済み":
        filtered_categories = categories_df[
            (categories_df['SEASONALLY_ADJUSTED'] == True) & 
            (categories_df['FREQUENCY'] == selected_frequency)
        ]
    elif seasonal_adjustment == "季節調整なし":
        filtered_categories = categories_df[
            (categories_df['SEASONALLY_ADJUSTED'] == False) & 
            (categories_df['FREQUENCY'] == selected_frequency)
        ]
    else:
        filtered_categories = categories_df[categories_df['FREQUENCY'] == selected_frequency]
    
    # 商品選択方式
    st.sidebar.subheader("🎯 分析対象選択")
    
    selection_mode = st.sidebar.radio(
        "選択方式",
        ["📊 主要カテゴリー（推奨）", "🔍 個別商品選択"],
        help="主要カテゴリーは専門的な経済分析に最適です"
    )
    
    available_products = sorted(filtered_categories['PRODUCT'].unique())
    
    if not available_products:
        st.error("選択した条件に該当する商品がありません。")
        st.stop()
    
    # 変数を初期化
    selected_categories = []
    
    if selection_mode == "📊 主要カテゴリー（推奨）":
        # 主要カテゴリー選択（プロフェッショナル推奨）
        major_categories = get_major_cpi_categories()
        selected_categories = st.sidebar.multiselect(
            "分析対象カテゴリー（複数選択可）",
            list(major_categories.keys()),
            default=list(major_categories.keys()),  # 全カテゴリーをデフォルト選択
            help="プロフェッショナルな経済分析で使用される主要カテゴリー"
        )
        
        # 選択されたカテゴリーから商品リストを作成
        selected_products = []
        for category in selected_categories:
            if category in major_categories:
                # カテゴリーの商品が利用可能な商品リストに含まれているかチェック
                category_products = major_categories[category]['products']
                available_category_products = [p for p in category_products if p in available_products]
                selected_products.extend(available_category_products)
        selected_products = list(set(selected_products))  # 重複除去
        
    else:
        # 個別商品選択（従来方式）
        default_products = []
        for product in ["All items", "All items less food and energy", "Food", "Energy"]:
            if product in available_products:
                default_products.append(product)
        
        selected_products = st.sidebar.multiselect(
            "分析対象商品（複数選択可）",
            available_products,
            default=default_products,
            help="個別の商品・サービスを直接選択"
        )
    
    # 商品数制限を削除（全商品表示を許可）
    # if len(selected_products) > 8:
    #     st.sidebar.warning("⚠️ 選択できる商品は最大8個までです。")
    #     selected_products = selected_products[:8]

    if not selected_products:
        st.warning("⚠️ 分析する商品・サービスを選択してください。")
        st.stop()

    # チャート表示オプション
    st.sidebar.subheader("📊 チャート表示設定")
    chart_style = st.sidebar.radio(
        "CPI推移表示形式",
        ["積み上げヒストグラム（前年同月比）", "積み上げヒストグラム（前月比）", "従来のライン表示"],
        index=0,
        help="積み上げヒストグラム表示が推奨です"
    )
    
    
    # 分析オプション
    st.sidebar.subheader("🔧 分析オプション")
    # show_all_items_baseline チェックボックスを削除（分析対象カテゴリーに含まれるため）
    show_inflation_rates = st.sidebar.checkbox("📊 インフレ率分析", value=True)
    show_ai_analysis = st.sidebar.checkbox("🤖 AI経済分析セクション表示", value=False)
    show_ai_agg_analysis = st.sidebar.checkbox("🧠 AI_AGG カテゴリー分析セクション表示", value=False)
    show_statistical_analysis = st.sidebar.checkbox("📈 統計分析", value=False)
    
    # AI モデル選択
    if show_ai_analysis or show_ai_agg_analysis:
        ai_model = st.sidebar.selectbox(
            "AIモデル選択",
            ["llama3.1-70b", "claude-3-5-sonnet", "mistral-large2"],
            index=1,
            help="Claude-3-5-Sonnetが最も詳細な経済分析を提供します"
        )
    else:
        ai_model = "llama3.1-70b"  # デフォルト値
    
    # 変数コード取得
    selected_variables = []
    for product in selected_products:
        product_vars = filtered_categories[filtered_categories['PRODUCT'] == product]['VARIABLE'].tolist()
        selected_variables.extend(product_vars)
    
    # データ読み込み
    with st.spinner("📊 CPI時系列データを読み込み中..."):
        timeseries_df = load_cpi_timeseries(selected_variables, start_date, end_date)
    
    if timeseries_df.empty:
        st.error("選択した期間・商品のCPIデータが見つかりませんでした。")
        st.stop()

    # チャートスタイル選択
    # chart_style = st.sidebar.selectbox(
    #     "📈 チャート表示形式",
    #     ["積み上げヒストグラム（前年同月比）", "積み上げヒストグラム（前月比）", "従来の線グラフ"],
    #     help="分析用途に応じたチャート形式を選択"
    # )

    # CPI推移チャート（積み上げヒストグラム形式）
    st.markdown('<div class="section-title">📈 CPI推移分析</div>', unsafe_allow_html=True)
    
    # 各商品のインフレ指標計算（テーブル表示用）
    inflation_data = {}
    
    if selection_mode == "📊 主要カテゴリー（推奨）":
        # 主要カテゴリー選択時：各カテゴリーの主要商品のみを使用
        major_categories = get_major_cpi_categories()
        for category in selected_categories:
            if category in major_categories:
                # 各カテゴリーの最初の商品（主要商品）のみを取得
                main_product = major_categories[category]['products'][0]
                product_data = timeseries_df[timeseries_df['PRODUCT'] == main_product]
                if not product_data.empty:
                    inflation_data[main_product] = calculate_inflation_metrics(product_data, main_product)
    else:
        # 個別商品選択時：選択された全商品を使用
        for product in selected_products:
            product_data = timeseries_df[timeseries_df['PRODUCT'] == product]
            if not product_data.empty:
                inflation_data[product] = calculate_inflation_metrics(product_data, product)

    # All Itemsベースライン重ね表示オプションを削除（分析対象カテゴリーに含まれるため）
    # overlay_baseline = st.checkbox(
    #     "📈 All Items CPI ベースラインを重ね表示", 
    #     value=show_all_items_baseline and 'All items' in selected_products,
    #     help="積み上げヒストグラムにAll Items CPIの前年同月比ラインを重ねて表示"
    # )

    # チャート作成 - 選択モードに応じて適切な関数を使用
    if selection_mode == "📊 主要カテゴリー（推奨）":
        # 主要カテゴリー選択時
        if chart_style == "積み上げヒストグラム（前年同月比）":
            # 寄与度分析チャートを使用（Bloomberg/X風）
            with st.spinner("🔢 寄与度を計算中..."):
                # 寄与度分析専用のデータ読み込み
                contribution_timeseries_df = load_cpi_timeseries_for_contribution(start_date, end_date, selected_frequency)
                
                if not contribution_timeseries_df.empty:
                    st.success(f"✅ 寄与度分析用データ読み込み完了: {len(contribution_timeseries_df['PRODUCT'].unique())}個の商品データを取得")
                    contribution_df = calculate_contribution_data_for_categories(contribution_timeseries_df, selected_categories, user_start_date=start_date)
                    
                    if not contribution_df.empty:
                        st.success(f"✅ 寄与度分析データ準備完了: {len(contribution_df)}行のデータを生成")
                        cpi_chart = create_bloomberg_contribution_chart_integrated(contribution_df, selected_categories)
                        # 寄与度分析の説明を追加（選択カテゴリーに応じて動的に変更）
                        category_names = []
                        line_names = []
                        
                        category_mapping = {
                            '⚡ Energy': 'Energy',
                            '🍎 Food': 'Food', 
                            '📦 Core Goods': 'Core Goods',
                            '🏠 Core Services': 'Core Services'
                        }
                        
                        for selected_cat in selected_categories:
                            if selected_cat in category_mapping:
                                category_names.append(category_mapping[selected_cat])
                            elif selected_cat == '📈 All Items':
                                line_names.append('All Items CPI')
                            elif selected_cat == '🎯 Core CPI':
                                line_names.append('Core CPI')
                        
                        # 説明文を構築
                        parts = []
                        if category_names:
                            categories_text = ', '.join(category_names)
                            parts.append(f"各カテゴリー（{categories_text}）の寄与度")
                        
                        if line_names:
                            lines_text = ', '.join(line_names)
                            parts.append(f"{lines_text}の前年同期比ライン")
                        
                        if parts:
                            description = ' と '.join(parts)
                            st.info(f"📊 **寄与度分析**: {description}をBloomberg Professional風に表示しています。")
                        else:
                            st.info("📊 **寄与度分析**: 各カテゴリーが全体のインフレ率に与える寄与度をBloomberg Professional風に表示しています。")
                    else:
                        st.warning("⚠️ 寄与度計算でデータが生成されませんでした。より長い期間を選択してください（12ヶ月以上推奨）。")
                        cpi_chart = create_major_category_chart(timeseries_df, selected_categories, "yoy")
                else:
                    st.warning("⚠️ 寄与度分析用データが取得できませんでした。期間を調整してみてください。")
                    cpi_chart = create_major_category_chart(timeseries_df, selected_categories, "yoy")
        elif chart_style == "積み上げヒストグラム（前月比）":
            cpi_chart = create_major_category_chart(timeseries_df, selected_categories, "mom")
        else:
            # 従来のライン表示：選択されたカテゴリーの主要商品のみ表示
            major_categories = get_major_cpi_categories()
            main_products = []
            for category in selected_categories:
                if category in major_categories:
                    # 各カテゴリーの最初の商品のみを取得（主要商品）
                    products_list = major_categories[category]['products']
                    if products_list:
                        main_products.append(products_list[0])  # 主要商品のみ
            
            # 重複削除と実際にデータが存在する商品のみフィルタ
            unique_products = list(set(main_products))
            available_products = [p for p in unique_products if p in timeseries_df['PRODUCT'].unique()]
            
            cpi_chart = create_cpi_comparison_chart(timeseries_df, available_products)
    else:
        # 個別商品選択時
        st.info(f"🔍 個別商品選択モード: {len(selected_products)}個の商品を分析中...")
        
        # デバッグ情報
        if timeseries_df.empty:
            st.error("❌ 時系列データが空です")
        else:
            available_products_in_data = timeseries_df['PRODUCT'].unique().tolist()
            st.info(f"📊 データに含まれる商品: {len(available_products_in_data)}個")
            missing_products = [p for p in selected_products if p not in available_products_in_data]
            if missing_products:
                st.warning(f"⚠️ データに含まれていない商品: {missing_products}")
        
        if chart_style == "積み上げヒストグラム（前年同月比）":
            cpi_chart = create_individual_products_chart(timeseries_df, selected_products, "yoy")
        elif chart_style == "積み上げヒストグラム（前月比）":
            cpi_chart = create_individual_products_chart(timeseries_df, selected_products, "mom")
        else:
            cpi_chart = create_cpi_comparison_chart(timeseries_df, selected_products)
    
    st.plotly_chart(cpi_chart, use_container_width=True)
    
    # データテーブル表示
    st.markdown('<div class="section-title">📊 データテーブル</div>', unsafe_allow_html=True)
    
    if selection_mode == "📊 主要カテゴリー（推奨）" and selected_categories:
        # 主要カテゴリーのテーブルデータ
        table_data = []
        major_categories = get_major_cpi_categories()
        
        for category in selected_categories:
            if category in major_categories:
                category_info = major_categories[category]
                # 各カテゴリーの主要商品（最初の商品）のみを使用
                main_product = category_info['products'][0]
                product_data = timeseries_df[timeseries_df['PRODUCT'] == main_product]
                
                if not product_data.empty:
                    # 最新データを取得
                    latest_row = product_data.sort_values('DATE').iloc[-1]
                    metrics = calculate_inflation_metrics(product_data, main_product)
                    
                    table_data.append({
                        'カテゴリー': category.split(' ', 1)[1],
                        '商品名': main_product,
                        '最新CPI': f"{latest_row['VALUE']:.1f}",
                        '前年同月比': f"{metrics.get('yearly_change', 0):.2f}%",
                        '前月比': f"{metrics.get('monthly_change', 0):.2f}%",
                        '最終更新': latest_row['DATE'].strftime('%Y-%m')
                    })
        
        if table_data:
            table_df = pd.DataFrame(table_data)
            st.dataframe(table_df, use_container_width=True, hide_index=True)
    else:
        # 個別商品選択時のテーブルデータ（概要情報統合版）
        table_data = []
        for product in selected_products:
            product_data = timeseries_df[timeseries_df['PRODUCT'] == product]
            if not product_data.empty:
                latest_row = product_data.sort_values('DATE').iloc[-1]
                metrics = inflation_data.get(product, {})
                
                # 色分け判定用のメトリクス
                yearly_change = metrics.get('yearly_change', 0)
                if yearly_change > 3:
                    trend_status = "⬆️ 高インフレ"
                elif yearly_change > 1:
                    trend_status = "📈 適度"
                elif yearly_change > 0:
                    trend_status = "📊 低水準"
                else:
                    trend_status = "⬇️ デフレ"
                
                table_data.append({
                    '商品・サービス': product,
                    '現在CPI水準': f"{latest_row['VALUE']:.1f}",
                    '前年同月比(%)': f"{metrics.get('yearly_change', 0):+.2f}",
                    '前月比(%)': f"{metrics.get('monthly_change', 0):+.2f}",
                    'ボラティリティ(%)': f"{metrics.get('volatility', 0):.2f}" if metrics.get('volatility') else "N/A",
                    'インフレ評価': trend_status,
                    '最終更新': latest_row['DATE'].strftime('%Y-%m'),
                    '季節調整': '✅' if latest_row.get('SEASONALLY_ADJUSTED', False) else '❌'
                })
        
        if table_data:
            table_df = pd.DataFrame(table_data)
            st.dataframe(table_df, use_container_width=True, hide_index=True)
    
    # 月別詳細内訳表示 - 削除
    # if show_detailed_breakdown and len(selected_products) <= 5:
    #     st.markdown('<div class="section-title">📈 商品別月次YoY%詳細分析</div>', unsafe_allow_html=True)
    #     st.markdown("""
    #     <div class="info-box">
    #     <b>エコノミスト向け詳細分析:</b> 各商品の月次前年同月比変化を個別に表示。
    #     インフレーション圧力の商品別寄与度を詳細に把握できます。
    #     </div>
    #     """, unsafe_allow_html=True)
    #     
    #     monthly_breakdown_chart = create_monthly_yoy_breakdown_chart(timeseries_df, selected_products)
    #     st.plotly_chart(monthly_breakdown_chart, use_container_width=True)
    # elif show_detailed_breakdown and len(selected_products) > 5:
    #     st.warning("⚠️ 月別詳細内訳は商品選択数を5個以下にしてください。")
    
    # インフレ率分析
    if show_inflation_rates and inflation_data:
        st.markdown('<div class="section-title">📊 インフレ率分析</div>', unsafe_allow_html=True)
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            inflation_chart = create_inflation_rate_chart(inflation_data)
            st.plotly_chart(inflation_chart, use_container_width=True)
        
        with col2:
            st.markdown("#### 📋 インフレ率詳細")
            
            # 商品を3列に分けてコンパクト表示
            products_list = list(inflation_data.keys())
            if len(products_list) <= 3:
                # 3商品以下の場合は1行で表示
                detail_cols = st.columns(len(products_list))
                for i, (product, metrics) in enumerate(inflation_data.items()):
                    if metrics:
                        with detail_cols[i]:
                            yearly_change = metrics.get('yearly_change', 0)
                            monthly_change = metrics.get('monthly_change', 0)
                            
                            st.markdown(f"**{product[:15]}...**" if len(product) > 15 else f"**{product}**")
                            st.metric("前年同月比", f"{yearly_change:+.1f}%")
                            st.metric("前月比", f"{monthly_change:+.1f}%")
                            
                            # コンパクトなインフレ評価
                            if yearly_change > 3:
                                st.markdown("🔴 高インフレ")
                            elif yearly_change > 2:
                                st.markdown("🟡 目標上回り")
                            elif yearly_change > 1:
                                st.markdown("🟢 目標近辺")
                            elif yearly_change > -1:
                                st.markdown("🔵 低インフレ")
                            else:
                                st.markdown("⚫ デフレ圧力")
            else:
                # 4商品以上の場合はexpanderでコンパクト表示
                for product, metrics in inflation_data.items():
                    if metrics:
                        yearly_change = metrics.get('yearly_change', 0)
                        monthly_change = metrics.get('monthly_change', 0)
                        volatility = metrics.get('volatility', 0)
                        
                        # インフレ評価アイコン
                        if yearly_change > 3:
                            icon = "🔴"
                        elif yearly_change > 2:
                            icon = "🟡"
                        elif yearly_change > 1:
                            icon = "🟢"
                        elif yearly_change > -1:
                            icon = "🔵"
                        else:
                            icon = "⚫"
                        
                        with st.expander(f"{icon} {product[:20]}... ({yearly_change:+.1f}%)" if len(product) > 20 else f"{icon} {product} ({yearly_change:+.1f}%)"):
                            col_a, col_b = st.columns(2)
                            with col_a:
                                st.metric("前年同月比", f"{yearly_change:+.2f}%")
                            with col_b:
                                st.metric("前月比", f"{monthly_change:+.2f}%")
                            
                            if volatility:
                                st.write(f"**ボラティリティ:** {volatility:.2f}%")
    
    # AI経済分析（プロフェッショナルスタイル）
    if show_ai_analysis:
        st.markdown('<div class="section-title">🤖 AI経済分析</div>', unsafe_allow_html=True)
        st.markdown("""
        <div class="info-box">
        <b>エコノミスト級AI分析:</b>著名エコノミストのような
        専門的な分析スタイルでCPIデータを解釈します。
        </div>
        """, unsafe_allow_html=True)
        
        if st.button("🧠 AI経済分析を実行", key="ai_analysis_button", help="AI_COMPLETE関数でエコノミスト級の分析を実行"):
            with st.spinner("🧠 AI分析を実行中..."):
                ai_analysis = generate_ai_analysis(selected_products, inflation_data, ai_model)
                # AI経済分析結果をsession_stateに保存
                st.session_state['ai_economic_analysis'] = ai_analysis
        
        # AI経済分析結果を表示（session_stateから取得）
        if 'ai_economic_analysis' in st.session_state:
            st.markdown(f"""
            <div class="info-box">
            <h4>🎯 エコノミスト級AI分析結果</h4>
            <p style="font-size: 1.1em; line-height: 1.6em;">{st.session_state['ai_economic_analysis']}</p>
            </div>
            """, unsafe_allow_html=True)

    # AI_AGG カテゴリー分析
    if show_ai_agg_analysis:
        st.markdown('<div class="section-title">🧠 AI_AGG 深層分析</div>', unsafe_allow_html=True)
        
        if selection_mode == "📊 主要カテゴリー（推奨）":
            st.markdown('<div class="info-box">💡 AI_AGG関数を使用してカテゴリー別に大量データを集約分析します。プロフェッショナルな経済分析をご覧ください。</div>', unsafe_allow_html=True)
            
            if st.button("🧠 AI_AGG カテゴリー分析を実行", key="ai_agg_button", help="AI_AGG関数で各カテゴリーの深層分析を実行"):
                # AI_AGG分析結果をsession_stateに保存
                if 'ai_agg_results' not in st.session_state:
                    st.session_state['ai_agg_results'] = {}
                
                # 選択されたカテゴリーごとにAI_AGG分析
                major_categories = get_major_cpi_categories()
                
                for category in selected_categories:
                    if category in major_categories:
                        category_products = major_categories[category]['products']
                        category_data = timeseries_df[timeseries_df['PRODUCT'].isin(category_products)]
                        
                        if not category_data.empty:
                            with st.spinner(f"{category}のAI_AGG分析を実行中..."):
                                ai_agg_result = analyze_cpi_data_with_ai_agg(
                                    category_data, 
                                    category.replace("📈 ", "").replace("🎯 ", "").replace("🍎 ", "").replace("⚡ ", "").replace("📦 ", "").replace("🏠 ", ""),
                                    category_products
                                )
                                # カテゴリー別に結果を保存
                                st.session_state['ai_agg_results'][category] = {
                                    'result': ai_agg_result,
                                    'color': major_categories[category]['color']
                                }
            
            # AI_AGG分析結果を表示（session_stateから取得）
            if 'ai_agg_results' in st.session_state and st.session_state['ai_agg_results']:
                for category, data in st.session_state['ai_agg_results'].items():
                    if category in selected_categories:  # 現在選択されているカテゴリーのみ表示
                        st.markdown(f"### {category}")
                        st.markdown(f"""
                        <div style='background-color: {data['color']}15; 
                                    padding: 1.5rem; 
                                    border-radius: 0.5rem; 
                                    border-left: 4px solid {data['color']};
                                    margin: 1rem 0;'>
                            <h4 style='color: {data['color']}; margin-top: 0;'>
                                {category} - 専門分析結果
                            </h4>
                            <p style='white-space: pre-wrap; line-height: 1.6;'>{data['result']}</p>
                        </div>
                        """, unsafe_allow_html=True)
                        st.markdown("---")
        else:
            st.markdown('<div class="info-box">💡 AI_AGG関数を使用して選択商品の大量データを集約分析します。個別商品ごとの専門的な経済分析をご覧ください。</div>', unsafe_allow_html=True)
            
            if st.button("🧠 AI_AGG 商品分析を実行", key="ai_agg_individual_button", help="AI_AGG関数で各商品の深層分析を実行"):
                # AI_AGG分析結果をsession_stateに保存
                if 'ai_agg_individual_results' not in st.session_state:
                    st.session_state['ai_agg_individual_results'] = {}
                
                # 選択された商品ごとにAI_AGG分析
                colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b']
                
                for i, product in enumerate(selected_products):
                    product_data = timeseries_df[timeseries_df['PRODUCT'] == product]
                    
                    if not product_data.empty:
                        with st.spinner(f"{product}のAI_AGG分析を実行中..."):
                            ai_agg_result = analyze_cpi_data_with_ai_agg(
                                product_data, 
                                product,
                                [product]
                            )
                            # 商品別に結果を保存
                            color = colors[i % len(colors)]
                            st.session_state['ai_agg_individual_results'][product] = {
                                'result': ai_agg_result,
                                'color': color
                            }
            
            # AI_AGG個別分析結果を表示（session_stateから取得）
            if 'ai_agg_individual_results' in st.session_state and st.session_state['ai_agg_individual_results']:
                for product, data in st.session_state['ai_agg_individual_results'].items():
                    if product in selected_products:  # 現在選択されている商品のみ表示
                        st.markdown(f"### 📊 {product}")
                        st.markdown(f"""
                        <div style='background-color: {data['color']}15; 
                                    padding: 1.5rem; 
                                    border-radius: 0.5rem; 
                                    border-left: 4px solid {data['color']};
                                    margin: 1rem 0;'>
                            <h4 style='color: {data['color']}; margin-top: 0;'>
                                {product} - 専門分析結果
                            </h4>
                            <p style='white-space: pre-wrap; line-height: 1.6;'>{data['result']}</p>
                        </div>
                        """, unsafe_allow_html=True)
                        st.markdown("---")

    # 統計分析
    if show_statistical_analysis:
        st.markdown('<div class="section-title">📊 統計分析</div>', unsafe_allow_html=True)
        
        # 相関分析
        if len(selected_products) > 1:
            # 相関行列（大きく表示）
            st.markdown("#### 📈 商品間価格相関")
            
            # 相関行列計算
            correlation_data = {}
            for product in selected_products:
                product_data = timeseries_df[timeseries_df['PRODUCT'] == product]
                if not product_data.empty:
                    product_data = product_data.sort_values('DATE')
                    correlation_data[product] = product_data.set_index('DATE')['VALUE']
            
            if len(correlation_data) > 1:
                corr_df = pd.DataFrame(correlation_data).corr()
                
                # ヒートマップ作成（大きく表示）
                fig_corr = px.imshow(
                    corr_df,
                    text_auto=True,
                    aspect="auto",
                    color_continuous_scale="RdBu_r",
                    title="商品間価格相関ヒートマップ"
                )
                fig_corr.update_layout(
                    height=600,  # 高さを増加
                    title={
                        'x': 0,
                        'font': {'size': 16}
                    }
                )
                st.plotly_chart(fig_corr, use_container_width=True)
            
            # 統計サマリー（相関行列の下に配置）
            st.markdown("#### 📊 統計サマリー")
            
            # 統計サマリー表作成
            summary_data = []
            for product, metrics in inflation_data.items():
                if metrics:
                    summary_data.append({
                        '商品・サービス': product,
                        '現在水準': f"{metrics.get('current_level', 0):.1f}",
                        '前年同月比(%)': f"{metrics.get('yearly_change', 0):+.2f}",
                        '前月比(%)': f"{metrics.get('monthly_change', 0):+.2f}",
                        'ボラティリティ(%)': f"{metrics.get('volatility', 0):.2f}" if metrics.get('volatility') else "N/A"
                    })
            
            if summary_data:
                summary_df = pd.DataFrame(summary_data)
                st.dataframe(summary_df, use_container_width=True)
        else:
            st.info("📈 相関分析には2つ以上の商品を選択してください。")
    
    # データエクスポート
    st.markdown('<div class="section-title">💾 データエクスポート</div>', unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("📋 分析データをCSVエクスポート"):
            # エクスポート用データ準備
            export_data = timeseries_df[['DATE', 'PRODUCT', 'VALUE', 'SEASONALLY_ADJUSTED', 'VARIABLE_NAME']]
            csv = export_data.to_csv(index=False)
            
            st.download_button(
                label="📥 CSVダウンロード",
                data=csv,
                file_name=f"cpi_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
    
    with col2:
        if st.button("📊 YoY%データをCSVエクスポート"):
            # YoY%データ準備
            yoy_export = []
            for product in selected_products:
                product_data = timeseries_df[timeseries_df['PRODUCT'] == product]
                if not product_data.empty:
                    yoy_data = calculate_yoy_monthly_data(product_data)
                    yoy_data['PRODUCT'] = product
                    yoy_export.append(yoy_data[['DATE', 'PRODUCT', 'VALUE', 'YoY_Change', 'MoM_Change']])
            
            if yoy_export:
                combined_yoy = pd.concat(yoy_export, ignore_index=True)
                csv = combined_yoy.to_csv(index=False)
                
                st.download_button(
                    label="📥 YoY%データダウンロード",
                    data=csv,
                    file_name=f"cpi_yoy_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
    
    # フッター
    st.markdown("---")
    st.markdown("""
    <div style='text-align: center; color: #666666; font-size: 0.9em;'>
    📊 CPI Analytics System | Powered by Snowflake & Streamlit | 
    Data: U.S. Bureau of Labor Statistics via Cybersyn<br>
    <i>Inspired by professional economic analysis style of leading economists</i>
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main() 
