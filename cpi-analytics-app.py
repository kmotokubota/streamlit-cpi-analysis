import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
import warnings

warnings.filterwarnings('ignore')

# --- ページ設定とSnowflakeセッション ---
st.set_page_config(
    page_title="CPI Analytics Dashboard for Finance Professionals",
    page_icon="🏦",
    layout="wide",
)

# Snowflakeセッションの取得
try:
    from snowflake.snowpark.context import get_active_session
    session = get_active_session()
    SNOWFLAKE_AVAILABLE = True
except Exception:
    SNOWFLAKE_AVAILABLE = False


# --- カスタムCSSによるデザイン刷新 ---
st.markdown("""
<style>
    /* 全体のフォントと背景 */
    .stApp {
        background-color: #FFFFFF;
    }
    /* メインヘッダー */
    .main-header {
        font-size: 2.8rem;
        font-weight: 700;
        color: #1E3A8A; /* 深い青 */
        text-align: center;
        margin-bottom: 1rem;
        padding-top: 1rem;
    }
    /* サブヘッダー */
    .sub-header {
        font-size: 1.1rem;
        color: #4B5563; /* グレー */
        text-align: center;
        margin-bottom: 2.5rem;
    }
    /* セクションタイトル */
    .section-title {
        font-size: 1.8rem;
        font-weight: 600;
        color: #374151;
        margin-top: 2rem;
        margin-bottom: 1rem;
        border-bottom: 2px solid #D1D5DB;
        padding-bottom: 0.5rem;
    }
    /* 情報ボックス */
    .info-box {
        background-color: #E0F2FE; /* 明るい青 */
        border-left: 5px solid #0EA5E9; /* スカイブルー */
        padding: 1.2rem;
        border-radius: 8px;
        margin: 1rem 0 2rem 0;
        font-size: 1.05em;
    }
    /* AI分析結果のボックス */
    .ai-analysis-box {
        background-color: #EFF6FF;
        border-left: 5px solid #60A5FA;
        padding: 1.5rem;
        border-radius: 8px;
        margin-top: 1rem;
    }
    .ai-analysis-box h4 {
        color: #B45309;
        margin-top: 0;
    }
    /* Streamlitコンポーネントの調整 */
    .stMetric {
        background-color: #FFFFFF;
        border-radius: 8px;
        padding: 1rem;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.05);
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 24px;
    }
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        white-space: pre-wrap;
        background-color: #FFFFFF;
        border-radius: 8px 8px 0 0;
        padding: 0 24px;
    }
    .stTabs [aria-selected="true"] {
        background-color: #1E3A8A;
        color: white;
    }
</style>
""", unsafe_allow_html=True)

@st.cache_data(ttl=600)
def load_cpi_attributes():
    """アプリで使用可能な全てのCPI属性リストを取得する"""
    if not SNOWFLAKE_AVAILABLE:
        return pd.DataFrame()
    query = """
    SELECT DISTINCT PRODUCT
    FROM FINANCE__ECONOMICS.CYBERSYN.BUREAU_OF_LABOR_STATISTICS_PRICE_ATTRIBUTES
    WHERE REPORT = 'Consumer Price Index'
      AND FREQUENCY = 'Monthly'
      AND SEASONALLY_ADJUSTED = TRUE
      AND PRODUCT IS NOT NULL;
    """
    try:
        return session.sql(query).to_pandas()
    except Exception as e:
        st.error(f"CPI属性データの取得に失敗しました: {e}")
        return pd.DataFrame()

# --- データ取得関数 (Snowflake) ---
@st.cache_data(ttl=600)
def load_cpi_timeseries_data(start_date, end_date):
    """寄与度分析とトレンド分析に必要なCPI時系列データをまとめて取得"""
    if not SNOWFLAKE_AVAILABLE:
        return pd.DataFrame()

    # YoY計算のために13ヶ月前からデータを取得
    extended_start_date = pd.to_datetime(start_date) - pd.DateOffset(months=13)

    query = f"""
    SELECT
        ts.DATE,
        ts.VALUE,
        attr.PRODUCT,
        attr.SEASONALLY_ADJUSTED
    FROM FINANCE__ECONOMICS.CYBERSYN.BUREAU_OF_LABOR_STATISTICS_PRICE_TIMESERIES ts
    JOIN FINANCE__ECONOMICS.CYBERSYN.BUREAU_OF_LABOR_STATISTICS_PRICE_ATTRIBUTES attr
      ON ts.VARIABLE = attr.VARIABLE
    WHERE attr.REPORT = 'Consumer Price Index'
      AND attr.FREQUENCY = 'Monthly'
      AND attr.SEASONALLY_ADJUSTED = TRUE
      AND ts.DATE BETWEEN '{extended_start_date.strftime('%Y-%m-%d')}' AND '{end_date.strftime('%Y-%m-%d')}'
      AND attr.PRODUCT IN (
          'All items', 'All items less food and energy', 'Food', 'Energy',
          'Services less energy services', 'Commodities less food and energy commodities'
      )
      AND ts.VALUE IS NOT NULL
    ORDER BY attr.PRODUCT, ts.DATE
    """
    try:
        df = session.sql(query).to_pandas()
        if not df.empty:
            df['DATE'] = pd.to_datetime(df['DATE'])
            df = df.sort_values(by=['PRODUCT', 'DATE'])
            # YoY と MoM を事前に計算
            df['YoY_Change'] = df.groupby('PRODUCT')['VALUE'].pct_change(periods=12) * 100
            df['MoM_Change'] = df.groupby('PRODUCT')['VALUE'].pct_change(periods=1) * 100
        return df
    except Exception as e:
        st.error(f"CPI時系列データの取得に失敗しました: {e}")
        return pd.DataFrame()


# --- 分析・計算関数 ---
@st.cache_data(ttl=600)
def calculate_contribution_data(_df, start_date):
    """CPI寄与度を計算"""
    if _df.empty:
        return pd.DataFrame()

    categories = {
        "Core Services": {"weight": 0.58, "product_name": "Services less energy services", "color": "#1E90FF"},
        "Core Goods": {"weight": 0.20, "product_name": "Commodities less food and energy commodities", "color": "#4682B4"},
        "Food": {"weight": 0.14, "product_name": "Food", "color": "#32CD32"},
        "Energy": {"weight": 0.08, "product_name": "Energy", "color": "#FF6347"}
    }
    
    # 寄与度を計算
    contribution_dfs = []
    for category, props in categories.items():
        cat_df = _df[_df['PRODUCT'] == props['product_name']].copy()
        cat_df['Contribution'] = cat_df['YoY_Change'] * props['weight']
        cat_df['Category'] = category
        cat_df['Color'] = props['color']
        contribution_dfs.append(cat_df[['DATE', 'Category', 'Contribution', 'Color']])

    result_df = pd.concat(contribution_dfs)
    
    # 全項目とコアCPIのYoY変化率をマージ
    all_items_yoy = _df[_df['PRODUCT'] == 'All items'][['DATE', 'YoY_Change']].rename(columns={'YoY_Change': 'All_Items_YoY'})
    core_cpi_yoy = _df[_df['PRODUCT'] == 'All items less food and energy'][['DATE', 'YoY_Change']].rename(columns={'YoY_Change': 'Core_CPI_YoY'})

    result_df = pd.merge(result_df, all_items_yoy, on='DATE', how='left')
    result_df = pd.merge(result_df, core_cpi_yoy, on='DATE', how='left')
    
    # NaNを除去し、表示期間でフィルタ
    result_df = result_df.dropna(subset=['Contribution']).reset_index(drop=True)
    return result_df[result_df['DATE'] >= pd.to_datetime(start_date)]


@st.cache_data(ttl=600)
def calculate_inflation_metrics(df):
    """DataFrameからインフレ指標（YoY, MoM）を計算して追加する"""
    if df.empty:
        return pd.DataFrame()
    
    # PRODUCT列が存在することを確認
    if 'PRODUCT' not in df.columns:
        st.warning("calculate_inflation_metrics: DataFrameにPRODUCT列がありません。")
        return df

    df = df.sort_values(by=['PRODUCT', 'DATE'])
    # groupby().pct_change() を使ってPRODUCTごとに計算
    df['YoY_Change'] = df.groupby('PRODUCT')['VALUE'].pct_change(periods=12) * 100
    df['MoM_Change'] = df.groupby('PRODUCT')['VALUE'].pct_change(periods=1) * 100
    return df

def get_major_cpi_products():
    """分析でよく使われる主要なCPI項目を返す"""
    return [
        "All items",
        "All items less food and energy",
        "Food",
        "Energy",
        "Commodities less food and energy commodities",
        "Services less energy services"
    ]

@st.cache_data
def get_latest_metrics(_df):
    """最新のKPI指標を取得"""
    metrics = {}
    products_to_track = ['All items', 'All items less food and energy', 'Food', 'Energy']
    for product in products_to_track:
        product_df = _df[_df['PRODUCT'] == product]
        if not product_df.empty:
            latest = product_df.iloc[-1]
            metrics[product] = {
                'YoY_Change': latest['YoY_Change'],
                'MoM_Change': latest['MoM_Change'],
                'Date': latest['DATE']
            }
    return metrics


# --- チャート生成の共通関数 ---
def get_professional_chart_layout(title, y_title, height=550):
    """Plotlyチャートのプロフェッショナルな共通レイアウトを生成"""
    return go.Layout(
        title={'text': title, 'x': 0.05, 'xanchor': 'left', 'font': {'size': 20, 'color': '#374151', 'family': 'Arial, sans-serif'}},
        yaxis={'title': y_title, 'gridcolor': '#E5E7EB'},
        xaxis={'gridcolor': '#E5E7EB'},
        plot_bgcolor='white',
        paper_bgcolor='white',
        legend={'orientation': 'h', 'yanchor': 'bottom', 'y': 1.02, 'xanchor': 'right', 'x': 1},
        height=height,
        margin={'l': 50, 'r': 50, 't': 80, 'b': 50}
    )

def calculate_dynamic_yrange(data_series_list):
    """複数のデータシリーズから動的なY軸範囲を計算"""
    valid_series = [s for s in data_series_list if s is not None and not s.empty]
    if not valid_series:
        return [-2, 10] # デフォルト
        
    combined_series = pd.concat(valid_series).dropna()
    
    if combined_series.empty:
        return [-2, 10]
    
    min_val, max_val = combined_series.min(), combined_series.max()
    
    # 無効な値が混入している場合
    if pd.isna(min_val) or pd.isna(max_val):
         return [-2, 10]

    # 全て同じ値の場合
    if min_val == max_val:
        min_val -= 1
        max_val += 1
        
    range_span = max_val - min_val
    margin = max(range_span * 0.1, 0.5) # 範囲の10%か最小0.5のマージン

    final_min = min_val - margin
    final_max = max_val + margin
    
    # データの最小値がゼロより大きい場合でも、Y軸の表示はゼロかそれ以下から開始
    if min_val >= 0:
        final_min = min(final_min, -0.1)

    # データの最大値がゼロより小さい場合でも、Y軸の表示はゼロかそれ以上まで確保
    if max_val <= 0:
        final_max = max(final_max, 0.1)

    return [final_min, final_max]


# --- チャート描画関数 ---
def create_contribution_chart(contrib_df):
    """寄与度分析チャートを作成"""
    if contrib_df.empty:
        return go.Figure()

    pivot_df = contrib_df.pivot(index='DATE', columns='Category', values='Contribution')
    category_order = ["Energy", "Food", "Core Goods", "Core Services"]
    
    fig = go.Figure()
    for category in category_order:
        if category in pivot_df.columns:
            color = contrib_df[contrib_df['Category'] == category]['Color'].iloc[0]
            fig.add_trace(go.Bar(
                name=category,
                x=pivot_df.index,
                y=pivot_df[category],
                marker_color=color,
                hovertemplate=f'<b>{category}</b><br>Date: %{{x}}<br>Contribution: %{{y:.2f}}pp<extra></extra>'
            ))
            
    line_data = contrib_df[['DATE', 'All_Items_YoY', 'Core_CPI_YoY']].drop_duplicates().set_index('DATE')
    fig.add_trace(go.Scatter(
        name='All Items CPI (YoY)', x=line_data.index, y=line_data['All_Items_YoY'],
        mode='lines+markers', line={'color': '#1E3A8A', 'width': 3}, marker_size=6,
        hovertemplate='<b>All Items CPI</b><br>YoY: %{y:.2f}%<extra></extra>'
    ))
    fig.add_trace(go.Scatter(
        name='Core CPI (YoY)', x=line_data.index, y=line_data['Core_CPI_YoY'],
        mode='lines+markers', line={'color': '#DC2626', 'width': 3, 'dash': 'dash'}, marker_size=6,
        hovertemplate='<b>Core CPI</b><br>YoY: %{y:.2f}%<extra></extra>'
    ))

    # Y軸の範囲を動的に設定
    positive_sums = pivot_df[pivot_df > 0].sum(axis=1)
    negative_sums = pivot_df[pivot_df < 0].sum(axis=1)
    y_range = calculate_dynamic_yrange([
        positive_sums,
        negative_sums,
        line_data['All_Items_YoY'],
        line_data['Core_CPI_YoY']
    ])

    layout = get_professional_chart_layout('CPI 前年同月比に対する各項目の寄与度', '寄与度 / YoY変化率 (%)')
    layout.barmode = 'relative'
    layout.yaxis.range = y_range
    layout.margin.t = 100
    fig.update_layout(layout)

    
    # ゼロラインを追加
    fig.add_hline(y=0, line_width=1, line_color="gray")
    
    # ターゲットラインを描画（テキストは別途追加）
    fig.add_hline(y=2, line_width=1, line_dash="dot", line_color="red")
    
    return fig

def create_trends_chart(trends_df, chart_type='YoY'):
    """主要項目のトレンドチャートを作成"""
    if trends_df.empty:
        return go.Figure()

    value_col = 'YoY_Change' if chart_type == 'YoY' else 'MoM_Change'
    title = f'主要CPI項目トレンド ({chart_type})'
    y_title = f'{chart_type} 変化率 (%)'

    fig = px.line(
        trends_df,
        x='DATE',
        y=value_col,
        color='PRODUCT',
        labels={'PRODUCT': '項目'},
        markers=True
    )
    
    # Y軸の範囲を動的に計算
    y_range = calculate_dynamic_yrange([trends_df[value_col]])
    
    layout = get_professional_chart_layout(title, y_title)
    layout.yaxis.range = y_range
    
    # 上部マージンを広げてタイトルと凡例の重なりを解消
    layout.margin.t = 170 
    
    fig.update_layout(layout)
    fig.add_hline(y=0, line_width=1, line_color="gray")
    return fig


# --- AI分析関数 (Snowflake Cortex) ---
def run_ai_complete_analysis(metrics, ai_model):
    """
    AI分析を生成（専門的な経済分析）。
    参照コードのプロンプト形式を参考に再構成。
    """
    if not SNOWFLAKE_AVAILABLE:
        return "Snowflakeセッションが利用できません。"
    
    try:
        # 分析データの要約を作成
        analysis_summary = []
        product_names = []
        for product, metric_values in metrics.items():
            if metric_values:
                product_names.append(product)
                yoy = metric_values.get('YoY_Change', 0)
                mom = metric_values.get('MoM_Change', 0)
                analysis_summary.append(f"【{product}】\n- 前年同月比: {yoy:+.2f}%\n- 前月比: {mom:+.2f}%")
        
        summary_text = "\n".join(analysis_summary)

        # 参照コードを基にした、より詳細なプロンプト
        prompt = f"""
        # 指示
        あなたはウォール街のトップエコノミストです。提供された最新の米国CPIデータを基に、プロフェッショナルな経済分析レポートを日本語で作成してください。

        # 分析対象データ
        - 分析対象項目: {', '.join(product_names)}
        - 最新の数値:
        {summary_text}
        
        # レポートに含めるべき内容 (5点)
        1. 各項目の価格動向の詳細な分析
        2. インフレの主要な変動要因の特定
        3. 経済全体へのインフレ圧力の根本的な評価
        4. この結果が米連邦準備制度(FRB)の金融政策に与える示唆
        5. 今後3～6ヶ月の見通しと主要なリスク要因

        # 出力形式
        - 各項目を明確に分けて、構造化された文章で記述してください。
        - 専門用語を適切に使い、客観的でデータに基づいた分析を行ってください。
        - 不要な改行は削除してください
        """

        safe_prompt = prompt.replace("'", "''")
        query = f"SELECT AI_COMPLETE('{ai_model}', '{safe_prompt}') AS analysis"
        result = session.sql(query).to_pandas()
        
        raw_analysis = result['ANALYSIS'].iloc[0]
        formatted_analysis = raw_analysis.replace('\\n', '\n')
        return formatted_analysis

    except Exception as e:
        return f"Cortex AI (COMPLETE)の分析でエラーが発生しました: {str(e)}"

        
def run_ai_agg_bulk_analysis(products_to_analyze, user_prompt):
    """
    AI_AGGとTRANSLATEを組み合わせ、ユーザー指定の観点で分析を実行する。
    """
    if not SNOWFLAKE_AVAILABLE or not products_to_analyze:
        return pd.DataFrame()

    product_list_str = "', '".join(products_to_analyze)
    safe_user_prompt = user_prompt.replace("'", "''")

    # AI_AGGで英語で分析 -> TRANSLATEで日本語に翻訳するクエリ
    query = f"""
    WITH timeseries_data AS (
        SELECT
            attr.PRODUCT,
            ts.DATE,
            ts.VALUE
        FROM FINANCE__ECONOMICS.CYBERSYN.BUREAU_OF_LABOR_STATISTICS_PRICE_TIMESERIES ts
        JOIN FINANCE__ECONOMICS.CYBERSYN.BUREAU_OF_LABOR_STATISTICS_PRICE_ATTRIBUTES attr
          ON ts.VARIABLE = attr.VARIABLE
        WHERE attr.PRODUCT IN ('{product_list_str}')
          AND attr.FREQUENCY = 'Monthly'
          AND attr.SEASONALLY_ADJUSTED = TRUE
          AND ts.DATE >= DATEADD(month, -24, CURRENT_DATE())
    )
    SELECT
        PRODUCT AS "項目名",
        SNOWFLAKE.CORTEX.TRANSLATE(
            AI_AGG(
                CONCAT(TO_VARCHAR(DATE, 'YYYY-MM'), ': ', VALUE),
                -- AI_AGGにはユーザーの質問を直接渡す
                '{safe_user_prompt}'
            ),
            'en', 'ja' -- 英語(en)から日本語(ja)へ翻訳
        ) AS "AIによるトレンド分析"
    FROM timeseries_data
    WHERE VALUE IS NOT NULL
    GROUP BY PRODUCT;
    """
    
    try:
        result_df = session.sql(query).to_pandas()
        # 結果のクリーニング処理
        def clean_ai_output(text):
            if isinstance(text, str):
                cleaned_text = text.replace('\\n', '\n').strip()
                return cleaned_text
            return text
        result_df["AIによるトレンド分析"] = result_df["AIによるトレンド分析"].apply(clean_ai_output)
        return result_df
    except Exception as e:
        st.error(f"Cortex AI (AI_AGG) の分析でエラーが発生しました: {e}")
        return pd.DataFrame()
        
# --- UI描画関数 ---
def render_sidebar():
    """サイドバーのUIを構築・描画"""
    with st.sidebar:
        st.image("https://avatars.githubusercontent.com/u/83149867?s=200&v=4", width=80)
        st.header("分析設定")
        
        today = datetime.now()
        start_date = st.date_input(
            "分析開始日",
            value=today - timedelta(days=365 * 3),
            min_value=today - timedelta(days=365 * 10),
            max_value=today
        )
        end_date = st.date_input("分析終了日", value=today, max_value=today)
        
        st.info("💡 現在の分析は季節調整済み・月次データに固定されています。")
        
        return start_date, end_date

def render_kpi_metrics(metrics):
    """主要KPIメトリクスを描画"""
    if not metrics:
        st.warning("KPIデータを計算できませんでした。")
        return

    st.markdown('<div class="section-title">📊 主要CPI指標（最新月）</div>', unsafe_allow_html=True)
    cols = st.columns(4)
    
    with cols[0]:
        st.metric(
            label=f"総合CPI (YoY) | {metrics['All items']['Date']:%Y-%m}",
            value=f"{metrics['All items']['YoY_Change']:.2f}%",
            delta=f"{metrics['All items']['MoM_Change']:.2f}% vs 前月",
        )
    with cols[1]:
        st.metric(
            label=f"コアCPI (YoY) | {metrics['All items less food and energy']['Date']:%Y-%m}",
            value=f"{metrics['All items less food and energy']['YoY_Change']:.2f}%",
            delta=f"{metrics['All items less food and energy']['MoM_Change']:.2f}% vs 前月",
        )
    with cols[2]:
        st.metric(
            label=f"食品 (YoY) | {metrics['Food']['Date']:%Y-%m}",
            value=f"{metrics['Food']['YoY_Change']:.2f}%",
            delta=f"{metrics['Food']['MoM_Change']:.2f}% vs 前月",
        )
    with cols[3]:
        st.metric(
            label=f"エネルギー (YoY) | {metrics['Energy']['Date']:%Y-%m}",
            value=f"{metrics['Energy']['YoY_Change']:.2f}%",
            delta=f"{metrics['Energy']['MoM_Change']:.2f}% vs 前月",
        )


# --- メインアプリケーション ---
def main():
    """アプリケーションのメイン実行関数"""
    st.markdown('<div class="main-header">🏦 U.S. CPI 分析ダッシュボード</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">金融プロフェッショナル向け | Powered by Snowflake Cortex AI ❄️</div>', unsafe_allow_html=True)

    if not SNOWFLAKE_AVAILABLE:
        st.error("⚠️ Snowflakeセッションに接続できません。Snowflake Native App環境で実行してください。")
        st.stop()
        
    start_date, end_date = render_sidebar()

    # --- データ読み込みと計算 ---
    cpi_attributes = load_cpi_attributes()
    if cpi_attributes.empty:
        st.error("CPIカテゴリデータの取得に失敗しました。")
        st.stop()

    with st.spinner("❄️ Snowflakeから最新のCPIデータを取得中..."):
        cpi_df = load_cpi_timeseries_data(start_date, end_date)

    if cpi_df.empty:
        st.error("データが取得できませんでした。期間を変更するか、管理者にお問い合わせください。")
        st.stop()
        
    with st.spinner("📈 インフレ指標を計算中..."):
        full_df = calculate_inflation_metrics(cpi_df.copy())
        contribution_df = calculate_contribution_data(full_df, start_date)
        latest_metrics = get_latest_metrics(full_df)

    # --- UI表示 ---
    render_kpi_metrics(latest_metrics)

    tab1, tab2, tab3, tab4 = st.tabs(["📈 寄与度分析", "📊 主要トレンド", "💡 AIによる洞察", "📄 データ詳細"])

    with tab1:
        st.markdown('<div class="section-title">総合インフレの要因分解</div>', unsafe_allow_html=True)
        st.markdown("""
        <div class="info-box">
        このチャートは、総合CPI（前年同月比）がどの構成要素（エネルギー、食品、コア財、コアサービス）によって変動したかを示します。
        棒グラフは各項目の「寄与度」を表し、それらの合計が総合CPIの動きと連動します。
        </div>
        """, unsafe_allow_html=True)
        contribution_chart = create_contribution_chart(contribution_df)
        st.plotly_chart(contribution_chart, use_container_width=True)

    with tab2:
        st.markdown('<div class="section-title">主要項目の価格トレンド</div>', unsafe_allow_html=True)
        chart_type = st.radio("表示する変化率", ["YoY", "MoM"], horizontal=True, key="trends_radio")
        
        trends_df = cpi_df[cpi_df['DATE'] >= pd.to_datetime(start_date)]
        trends_chart = create_trends_chart(trends_df, chart_type)
        st.plotly_chart(trends_chart, use_container_width=True)

    with tab3:
        st.markdown('<div class="section-title">Cortex AIによる経済分析</div>', unsafe_allow_html=True)
        st.markdown("""
        <div class="info-box">
        SnowflakeのCortex AI関数を活用し、データから専門的な洞察を自動生成します。<br>
        - <b>全体サマリー分析</b>: <code>AI_COMPLETE</code>を使い、主要KPIからマクロ経済の示唆を導出します。<br>
        - <b>複数項目の一括分析</b>: <code>AI_AGG</code>を使い、選択した全項目のトレンドを一度のクエリで個別に分析します。
        </div>
        """, unsafe_allow_html=True)

        col1, col2 = st.columns([1.2, 1]) 
        
        with col1:
            st.subheader("全体サマリー分析 (AI_COMPLETE)")

            ai_model_complete = st.selectbox(
                "🧠 使用するAIモデルを選択",
                ["llama4-maverick", "claude-4-sonnet", "claude-3-5-sonnet", "mistral-large2"],
                key="model_selector_complete"
            )
            
            if st.button("🧠 サマリー分析を実行", key="ai_complete_button"):
                with st.spinner(f"AI ({ai_model_complete}) が全体状況を分析中..."):
                    st.session_state.ai_summary = run_ai_complete_analysis(latest_metrics, ai_model_complete)
            
            if 'ai_summary' in st.session_state:
                st.markdown(f"""
                <div class="ai-analysis-box" style="white-space: pre-wrap;">
                    {st.session_state.ai_summary}
                </div>
                """, unsafe_allow_html=True)

        with col2:
            st.subheader("複数項目の一括分析 (AI_AGG)")

            # 分析観点の入力UI（参照コードを参考）
            agg_input_type = st.radio(
                "分析観点の指定方法:",
                ["サンプルから選択", "自由入力"],
                horizontal=True,
                key="agg_input_type"
            )

            if agg_input_type == "サンプルから選択":
                agg_prompts = [
                    "最近のトレンドを150字程度で要約して。",
                    "価格を押し上げている主な要因は何ですか？",
                    "価格を安定させている主な要因は何ですか？",
                    "この項目の価格変動は、他の経済指標（例：個人消費支出、生産者物価指数）にどのような影響を与えそうですか？",
                    "このデータは、FRBの次の金融政策決定会合（FOMC）にどのような影響を与えますか？"
                ]
                selected_agg_prompt = st.selectbox("分析観点を選択:", agg_prompts)
            else: # 自由入力
                selected_agg_prompt = st.text_input(
                    "分析したい観点を入力 (日本語でOK):",
                    placeholder="例: この項目の特徴的な傾向は？"
                )

            # 分析対象の選択
            products_for_agg = ['All items', 'All items less food and energy', 'Food', 'Energy', 'Services less energy services', 'Commodities less food and energy commodities']
            selected_for_agg = st.multiselect(
                "分析対象の項目を選択してください:",
                options=products_for_agg,
                default=products_for_agg[:4] 
            )
            
            if st.button(f"🧠 {len(selected_for_agg)}項目を分析", key="ai_agg_button"):
                if not selected_agg_prompt or selected_agg_prompt.strip() == "":
                    st.error("分析観点を入力または選択してください。")
                else:
                    with st.spinner(f"AIが{len(selected_for_agg)}項目のトレンドを並列分析中..."):
                        st.session_state.ai_agg_results = run_ai_agg_bulk_analysis(selected_for_agg, selected_agg_prompt)

            # 分析結果の表示
            if 'ai_agg_results' in st.session_state and not st.session_state.ai_agg_results.empty:
                st.markdown("---")
                st.write(f"**分析結果：**{st.session_state.get('last_agg_prompt', '')}")
                
                results_df = st.session_state.ai_agg_results
                for index, row in results_df.iterrows():
                    item_name = row["項目名"]
                    analysis_text = row["AIによるトレンド分析"]
                    cleaned_text = analysis_text.replace('**', '').replace('*', '').replace('\\', '').replace('_', '').replace('#', '')
                    
                    st.markdown(f"""
                    <div class="ai-analysis-box" style="margin-bottom: 1rem; white-space: pre-wrap;">
                        <h5 style="margin-top:0; margin-bottom: 0.5rem;">{item_name}</h5>
                        {analysis_text}
                    </div>
                    """, unsafe_allow_html=True)

            # 実行時のプロンプトを保存（表示用）
            if 'ai_agg_results' in st.session_state:
                st.session_state['last_agg_prompt'] = selected_agg_prompt

    with tab4:
        st.markdown('<div class="section-title">📄 データ詳細</div>', unsafe_allow_html=True)

        all_products = sorted(cpi_attributes['PRODUCT'].unique())
        major_products = get_major_cpi_products()
        default_products = [p for p in major_products if p in all_products]

        selected_detail_products = st.multiselect(
            "表示するCPI項目を選択（複数選択可）:",
            options=all_products,
            default=default_products,
            key="multiselect_data_details"
        )

        if selected_detail_products:
            display_df = full_df[full_df['PRODUCT'].isin(selected_detail_products)]
            
            # 日付の新しい順に並び替え
            sorted_df = display_df.sort_values(by="DATE", ascending=False)

            # 修正点①: 古いインデックスをリセットして、1から始まる連番にする
            sorted_df = sorted_df.reset_index(drop=True)
            
            # 表示用にデータフレームをコピー
            df_for_display = sorted_df.copy()

            # 修正点②: 日付列の表示形式を 'YYYY-MM-DD' に変更
            df_for_display['DATE'] = df_for_display['DATE'].dt.strftime('%Y-%m-%d')

            # 整形したデータフレームを表示
            st.dataframe(
                df_for_display[['DATE', 'PRODUCT', 'VALUE', 'YoY_Change', 'MoM_Change']].rename(columns={
                    'DATE': '日付', 'PRODUCT': '項目', 'VALUE': 'CPI値',
                    'YoY_Change': '前年同月比(%)', 'MoM_Change': '前月比(%)'
                }),
                use_container_width=True,
                height=500
            )

            st.markdown("---")
            # ダウンロードするCSVは元のデータ（sorted_df）を使用
            csv_data = sorted_df.to_csv(index=False).encode('utf-8')
            st.download_button(
               label="📥 表示中のデータをCSVとしてダウンロード",
               data=csv_data,
               file_name=f"cpi_detail_data_{datetime.now().strftime('%Y%m%d')}.csv",
               mime="text/csv",
            )
        else:
            st.info("⬆️ 上のメニューから表示する項目を1つ以上選択してください。")
        
if __name__ == "__main__":
    main()
