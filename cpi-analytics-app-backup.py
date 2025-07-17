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
            "color": "#2E86AB",
            "display_type": "line"
        },
        "🎯 Core CPI": {
            "products": ["All items less food and energy", "All items less food, energy, and shelter"],
            "description": "コアCPI（食品・エネルギー除く）",
            "color": "#A23B72",
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
                'あなたは経済専門家のJoseph Brueselasです。提供された{category_name}のCPIデータを分析し、以下の観点から日本語で専門的な経済分析を行ってください：
                1. 最新の価格動向とインフレトレンド
                2. 前年同月比・前月比の変化パターン
                3. 経済政策への含意
                4. 今後の予測と注意すべきリスク要因
                5. この分野が全体のインフレに与える影響
                分析は具体的な数値を引用し、エコノミストらしい専門的な視点で回答してください。'
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

def create_cpi_histogram_chart(df, selected_products, chart_type="yoy"):
    """CPI推移を積み上げヒストグラム形式で作成"""
    if df.empty:
        return go.Figure()
    
    fig = go.Figure()
    
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', 
              '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf']
    
    for i, product in enumerate(selected_products):
        product_data = df[df['PRODUCT'] == product].copy()
        
        if not product_data.empty:
            # YoYデータを計算
            yoy_data = calculate_yoy_monthly_data(product_data)
            
            if chart_type == "yoy":
                # 前年同月比積み上げヒストグラム
                y_values = yoy_data['YoY_Change'].dropna()
                chart_title = ""
                y_title = "前年同月比 (%)"
            else:
                # 前月比積み上げヒストグラム
                y_values = yoy_data['MoM_Change'].dropna()
                chart_title = ""
                y_title = "前月比 (%)"
            
            if not y_values.empty:
                fig.add_trace(go.Bar(
                    x=yoy_data['DATE'].iloc[-len(y_values):],
                    y=y_values,
                    name=product,
                    marker_color=colors[i % len(colors)],
                    opacity=1.0,
                    hovertemplate=(
                        f'<b>{product}</b><br>'
                        '日付: %{x}<br>'
                        f'{y_title}: %{{y:.2f}}%<br>'
                        '<extra></extra>'
                    )
                ))
    
    fig.update_layout(
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

def create_major_category_chart(df, selected_categories, chart_type="yoy"):
    """主要カテゴリー用のチャート作成（バー+ライン）"""
    if df.empty:
        return go.Figure()
    
    fig = go.Figure()
    major_categories = get_major_cpi_categories()
    
    for category in selected_categories:
        if category in major_categories:
            category_info = major_categories[category]
            category_products = category_info['products']
            display_type = category_info['display_type']
            color = category_info['color']
            
            # カテゴリーに該当するデータを抽出
            category_data = df[df['PRODUCT'].isin(category_products)]
            
            if not category_data.empty:
                # 代表的な商品を選択（最初の商品または最もデータが多い商品）
                main_product = category_data.groupby('PRODUCT').size().idxmax()
                product_data = category_data[category_data['PRODUCT'] == main_product].copy()
                
                if not product_data.empty:
                    # YoYデータを計算
                    yoy_data = calculate_yoy_monthly_data(product_data)
                    
                    if chart_type == "yoy":
                        y_values = yoy_data['YoY_Change'].dropna()
                        y_title = "前年同月比 (%)"
                    else:
                        y_values = yoy_data['MoM_Change'].dropna()
                        y_title = "前月比 (%)"
                    
                    if not y_values.empty:
                        if display_type == "bar":
                            # バー表示
                            fig.add_trace(go.Bar(
                                x=yoy_data['DATE'].iloc[-len(y_values):],
                                y=y_values,
                                name=category.split(' ', 1)[1],  # 絵文字を除いた名前
                                marker_color=color,
                                opacity=0.8,
                                hovertemplate=(
                                    f'<b>{category}</b><br>'
                                    '日付: %{x}<br>'
                                    f'{y_title}: %{{y:.2f}}%<br>'
                                    '<extra></extra>'
                                )
                            ))
                        else:
                            # ライン表示
                            fig.add_trace(go.Scatter(
                                x=yoy_data['DATE'].iloc[-len(y_values):],
                                y=y_values,
                                mode='lines',
                                name=category.split(' ', 1)[1],  # 絵文字を除いた名前
                                line=dict(color=color, width=3),
                                hovertemplate=(
                                    f'<b>{category}</b><br>'
                                    '日付: %{x}<br>'
                                    f'{y_title}: %{{y:.2f}}%<br>'
                                    '<extra></extra>'
                                )
                            ))
    
    fig.update_layout(
        xaxis_title='期間',
        yaxis_title=y_title,
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
            'text': '📈 商品別月次CPI前年同月比詳細分析',
            'x': 0.5,
            'font': {'size': 20, 'family': 'Arial, sans-serif'}
        },
        height=200 * len(selected_products) + 100,
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
        Joseph Brusuelas氏のようなプロフェッショナルな分析スタイルで回答してください。

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
    """従来のCPI比較チャート（ライン形式）"""
    if df.empty:
        return go.Figure()
    
    fig = go.Figure()
    
    colors = px.colors.qualitative.Set1
    
    for i, product in enumerate(selected_products):
        product_data = df[df['PRODUCT'] == product].sort_values('DATE')
        
        if not product_data.empty:
            fig.add_trace(go.Scatter(
                x=product_data['DATE'],
                y=product_data['VALUE'],
                mode='lines+markers',
                name=product,
                line=dict(color=colors[i % len(colors)], width=2),
                marker=dict(size=4),
                hovertemplate=f'<b>{product}</b><br>日付: %{{x}}<br>CPI: %{{y:.1f}}<extra></extra>'
            ))
    
    fig.update_layout(
        title={
            'text': '📈 CPI商品別価格指数推移（従来表示）',
            'x': 0.5,
            'font': {'size': 18, 'family': 'Arial, sans-serif'}
        },
        xaxis_title='日付',
        yaxis_title='価格指数',
        hovermode='x unified',
        height=500,
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )
    
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
            'x': 0.5,
            'font': {'size': 18, 'family': 'Arial, sans-serif'}
        },
        xaxis_title='商品・サービス',
        yaxis_title='変化率 (%)',
        barmode='group',
        height=400,
        showlegend=True
    )
    
    # ゼロラインの追加
    fig.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.5)
    
    return fig

def main():
    """メイン関数"""
    
    # ヘッダー
    st.markdown('<h1 class="main-header">📊 CPI Analytics System</h1>', unsafe_allow_html=True)
    st.markdown('<div class="info-box">🏛️ <b>消費者物価指数分析システム</b> - エコノミスト向け包括的CPI分析ツール</div>', unsafe_allow_html=True)
    
    if not st.session_state.get('snowflake_available', False):
        st.stop()
    
    # サイドバー設定
    st.sidebar.header("📋 分析設定")
    
    # 日付範囲設定
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
    
    # CPIカテゴリ読み込み
    with st.spinner("📊 CPIカテゴリを読み込み中..."):
        categories_df = load_cpi_categories()
    
    if categories_df.empty:
        st.error("CPIカテゴリデータを読み込めませんでした。")
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
        index=frequency_options.index('Monthly') if 'Monthly' in frequency_options else 0
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
        # 主要カテゴリー選択
        major_categories = get_major_cpi_categories()
        
        st.sidebar.markdown("**💡 LinkedIn投稿スタイル分析**")
        selected_categories = st.sidebar.multiselect(
            "分析カテゴリーを選択",
            list(major_categories.keys()),
            default=["📈 All Items", "🎯 Core CPI", "🍎 Food", "⚡ Energy"],
            help="複数選択可能。All Itemsは常にベースライン表示されます"
        )
        
        # カテゴリー説明表示
        for category in selected_categories:
            if category in major_categories:
                st.sidebar.markdown(f"**{category}**: {major_categories[category]['description']}")
        
        # 選択されたカテゴリーから商品を抽出
        selected_products = []
        for category in selected_categories:
            if category in major_categories:
                for product in major_categories[category]['products']:
                    if product in available_products:
                        selected_products.append(product)
        
        # 重複削除
        selected_products = list(set(selected_products))
        
    else:
        # 個別商品選択（従来方式）
        # デフォルト選択（主要な項目）
        default_products = []
        key_products = ['All items', 'Food at home', 'Energy', 'Housing', 'Transportation', 'Medical care']
        for product in key_products:
            if product in available_products:
                default_products.append(product)
        
        if not default_products:
            default_products = available_products[:5]
        
        selected_products = st.sidebar.multiselect(
            "商品・サービスを選択",
            available_products,
            default=default_products,
            help="最大8個まで選択可能"
        )
        
        if len(selected_products) > 8:
            st.sidebar.warning("⚠️ 選択できる商品は最大8個までです。")
            selected_products = selected_products[:8]
    
    if not selected_products:
        st.warning("⚠️ 分析する商品・サービスを選択してください。")
        st.stop()
    
    # チャート表示オプション
    st.sidebar.subheader("📊 チャート表示設定")
    chart_style = st.sidebar.radio(
        "CPI推移表示形式",
        ["積み上げヒストグラム（前年同月比）", "積み上げヒストグラム（前月比）", "従来のライン表示"],
        index=0,
        help="LinkedInスタイルの積み上げヒストグラム表示が推奨です"
    )
    
    show_detailed_breakdown = st.sidebar.checkbox(
        "📈 月別詳細内訳表示", 
        value=True,
        help="商品別の月次YoY%詳細分析"
    )
    
    # 分析オプション
    st.sidebar.subheader("🔧 分析オプション")
    show_all_items_baseline = st.sidebar.checkbox("📈 All Items ベースライン重ね表示", value=True, help="CPI推移分析チャートにAll Items CPIの前年同月比ラインを重ねて表示")
    show_inflation_rates = st.sidebar.checkbox("📊 インフレ率分析", value=True)
    show_ai_analysis = st.sidebar.checkbox("🤖 AI経済分析セクション表示", value=False)
    show_ai_agg_analysis = st.sidebar.checkbox("🧠 AI_AGG カテゴリー分析セクション表示", value=False)
    show_statistical_analysis = st.sidebar.checkbox("📈 統計分析", value=True)
    
    # AI モデル選択
    if show_ai_analysis:
        ai_model = st.sidebar.selectbox(
            "AIモデル選択",
            ["llama3.1-70b", "claude-3-5-sonnet", "mistral-large2"],
            index=1,
            help="Claude-3-5-Sonnetが最も詳細な経済分析を提供します"
        )
    
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
    
    # メインダッシュボード
    st.markdown('<div class="main-header">📊 CPI分析ダッシュボード</div>', unsafe_allow_html=True)
    

    
    # 概要メトリクス
    st.markdown('<div class="section-title">📋 現在のCPI概要</div>', unsafe_allow_html=True)
    
    inflation_data = {}
    
    # 各商品のインフレ指標計算
    for product in selected_products:
        product_data = timeseries_df[timeseries_df['PRODUCT'] == product]
        if not product_data.empty:
            inflation_data[product] = calculate_inflation_metrics(product_data, product)
    
    # メトリクス表示
    cols = st.columns(min(len(selected_products), 4))
    for i, (product, metrics) in enumerate(inflation_data.items()):
        with cols[i % 4]:
            if metrics:
                yearly_change = metrics.get('yearly_change', 0)
                monthly_change = metrics.get('monthly_change', 0)
                current_level = metrics.get('current_level', 0)
                
                # 色分け判定
                if yearly_change > 3:
                    delta_color = "inverse"  # 赤
                elif yearly_change > 1:
                    delta_color = "normal"   # 緑
                else:
                    delta_color = "off"      # グレー
                
                st.metric(
                    label=product,
                    value=f"{current_level:.1f}",
                    delta=f"{yearly_change:+.2f}% (年率)",
                    help=f"前月比: {monthly_change:+.2f}%",
                    delta_color=delta_color
                )
    
    # CPI推移チャート（新しい積み上げヒストグラム形式）
    st.markdown('<div class="section-title">📈 CPI推移分析</div>', unsafe_allow_html=True)
    
    # All Itemsベースライン重ね表示オプション
    overlay_baseline = st.checkbox(
        "📈 All Items CPI ベースラインを重ね表示", 
        value=show_all_items_baseline and 'All items' in selected_products,
        help="積み上げヒストグラムにAll Items CPIの前年同月比ラインを重ねて表示"
    )
    
    if chart_style == "積み上げヒストグラム（前年同月比）":
        cpi_chart = create_cpi_histogram_chart(timeseries_df, selected_products, "yoy")
        
        # All Itemsベースラインを重ね表示
        if overlay_baseline and 'All items' in selected_products:
            cpi_chart = add_all_items_baseline_to_chart(cpi_chart, timeseries_df)
            
    elif chart_style == "積み上げヒストグラム（前月比）":
        cpi_chart = create_cpi_histogram_chart(timeseries_df, selected_products, "mom")
    else:
        cpi_chart = create_cpi_comparison_chart(timeseries_df, selected_products)
    
    st.plotly_chart(cpi_chart, use_container_width=True)
    
    # 月別詳細内訳表示
    if show_detailed_breakdown and len(selected_products) <= 5:
        st.markdown('<div class="section-title">📈 商品別月次YoY%詳細分析</div>', unsafe_allow_html=True)
        st.markdown("""
        <div class="info-box">
        <b>エコノミスト向け詳細分析:</b> 各商品の月次前年同月比変化を個別に表示。
        インフレーション圧力の商品別寄与度を詳細に把握できます。
        </div>
        """, unsafe_allow_html=True)
        
        monthly_breakdown_chart = create_monthly_yoy_breakdown_chart(timeseries_df, selected_products)
        st.plotly_chart(monthly_breakdown_chart, use_container_width=True)
    elif show_detailed_breakdown and len(selected_products) > 5:
        st.warning("⚠️ 月別詳細内訳は商品選択数を5個以下にしてください。")
    
    # インフレ率分析
    if show_inflation_rates and inflation_data:
        st.markdown('<div class="section-title">📊 インフレ率分析</div>', unsafe_allow_html=True)
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            inflation_chart = create_inflation_rate_chart(inflation_data)
            st.plotly_chart(inflation_chart, use_container_width=True)
        
        with col2:
            st.markdown("#### 📋 インフレ率詳細")
            for product, metrics in inflation_data.items():
                if metrics:
                    yearly_change = metrics.get('yearly_change', 0)
                    monthly_change = metrics.get('monthly_change', 0)
                    volatility = metrics.get('volatility', 0)
                    
                    with st.expander(f"📊 {product}"):
                        st.write(f"**前年同月比:** {yearly_change:+.2f}%")
                        st.write(f"**前月比:** {monthly_change:+.2f}%")
                        if volatility:
                            st.write(f"**ボラティリティ:** {volatility:.2f}%")
                        
                        # インフレ評価（LinkedInの投稿を参考）
                        if yearly_change > 3:
                            st.write("🔴 **高インフレ** (>3%)")
                            st.write("*FRBの2%目標を大幅に上回る*")
                        elif yearly_change > 2:
                            st.write("🟡 **目標上回り** (2-3%)")
                            st.write("*FRBの2%目標をやや上回る*")
                        elif yearly_change > 1:
                            st.write("🟢 **目標近辺** (1-2%)")
                            st.write("*FRBの2%目標に近い水準*")
                        elif yearly_change > -1:
                            st.write("🔵 **低インフレ** (0-1%)")
                            st.write("*非常に低いインフレ水準*")
                        else:
                            st.write("⚫ **デフレ圧力** (<0%)")
                            st.write("*価格下落圧力が存在*")
    
    # AI経済分析（LinkedInスタイル）
    if show_ai_analysis:
        st.markdown('<div class="section-title">🤖 AI経済分析</div>', unsafe_allow_html=True)
        st.markdown("""
        <div class="info-box">
        <b>エコノミスト級AI分析:</b> Joseph Brusuelas氏やその他の著名エコノミストのような
        専門的な分析スタイルでCPIデータを解釈します。
        </div>
        """, unsafe_allow_html=True)
        
        if st.button("🧠 AI経済分析を実行", key="ai_analysis_button", help="AI_COMPLETE関数でエコノミスト級の分析を実行"):
            with st.spinner("🧠 AI分析を実行中..."):
                ai_analysis = generate_ai_analysis(selected_products, inflation_data, ai_model)
            
            st.markdown(f"""
            <div class="info-box">
            <h4>🎯 エコノミスト級AI分析結果</h4>
            <p style="font-size: 1.1em; line-height: 1.6em;">{ai_analysis}</p>
            </div>
            """, unsafe_allow_html=True)
    
    # 統計分析
    if show_statistical_analysis:
        st.markdown('<div class="section-title">📊 統計分析</div>', unsafe_allow_html=True)
        
        # 相関分析
        if len(selected_products) > 1:
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("#### 📈 相関行列")
                
                # 相関行列計算
                correlation_data = {}
                for product in selected_products:
                    product_data = timeseries_df[timeseries_df['PRODUCT'] == product]
                    if not product_data.empty:
                        product_data = product_data.sort_values('DATE')
                        correlation_data[product] = product_data.set_index('DATE')['VALUE']
                
                if len(correlation_data) > 1:
                    corr_df = pd.DataFrame(correlation_data).corr()
                    
                    # ヒートマップ作成
                    fig_corr = px.imshow(
                        corr_df,
                        text_auto=True,
                        aspect="auto",
                        color_continuous_scale="RdBu_r",
                        title="商品間価格相関"
                    )
                    fig_corr.update_layout(height=400)
                    st.plotly_chart(fig_corr, use_container_width=True)
            
            with col2:
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
    
    # AI_AGG カテゴリー分析
    if show_ai_agg_analysis and selection_mode == "📊 主要カテゴリー（推奨）":
        st.markdown('<div class="section-title">🧠 AI_AGG カテゴリー別深層分析</div>', unsafe_allow_html=True)
        
        st.markdown('<div class="info-box">💡 AI_AGG関数を使用してカテゴリー別に大量データを集約分析します。Joseph Brueselasスタイルの専門的な経済分析をご覧ください。</div>', unsafe_allow_html=True)
        
        if st.button("🧠 AI_AGG カテゴリー分析を実行", key="ai_agg_button", help="AI_AGG関数で各カテゴリーの深層分析を実行"):
            # 選択されたカテゴリーごとにAI_AGG分析
            major_categories = get_major_cpi_categories()
            
            for category in selected_categories:
                if category in major_categories:
                    category_products = major_categories[category]['products']
                    category_data = timeseries_df[timeseries_df['PRODUCT'].isin(category_products)]
                    
                    if not category_data.empty:
                        st.markdown(f"### {category}")
                        
                        with st.spinner(f"{category}のAI_AGG分析を実行中..."):
                            ai_agg_result = analyze_cpi_data_with_ai_agg(
                                category_data, 
                                category.replace("📈 ", "").replace("🎯 ", "").replace("🍎 ", "").replace("⚡ ", "").replace("📦 ", "").replace("🏠 ", ""),
                                category_products
                            )
                        
                        # 結果表示
                        st.markdown(f"""
                        <div style='background-color: {major_categories[category]['color']}15; 
                                    padding: 1.5rem; 
                                    border-radius: 0.5rem; 
                                    border-left: 4px solid {major_categories[category]['color']};
                                    margin: 1rem 0;'>
                            <h4 style='color: {major_categories[category]['color']}; margin-top: 0;'>
                                {category} - 専門分析結果
                            </h4>
                            <p style='white-space: pre-wrap; line-height: 1.6;'>{ai_agg_result}</p>
                        </div>
                        """, unsafe_allow_html=True)
                        
                        st.markdown("---")
    
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