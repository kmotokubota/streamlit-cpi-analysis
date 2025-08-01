"""
CPI Analytics System - Main Application
消費者物価指数（CPI）の専門的分析システム
"""

import streamlit as st
import pandas as pd
from datetime import datetime, timedelta

# モジュールインポート
from config.settings import (
    DEFAULT_SETTINGS, MAJOR_CPI_CATEGORIES, AI_SETTINGS
)
from data.loader import (
    get_snowflake_session, load_cpi_categories, 
    load_cpi_timeseries, load_cpi_timeseries_for_contribution
)
from data.processor import (
    calculate_inflation_metrics, filter_data_by_criteria,
    prepare_products_for_category_selection, prepare_table_data_for_categories,
    prepare_table_data_for_products, prepare_export_data
)
from visualization.charts import (
    create_stacked_histogram, create_monthly_yoy_breakdown_chart,
    create_major_category_chart, create_bloomberg_contribution_chart_integrated,
    create_inflation_rate_chart, add_all_items_baseline_to_chart
)
from analysis.ai_analyzer import (
    generate_ai_analysis, generate_category_ai_analysis,
    generate_individual_product_analysis, format_ai_analysis_result
)
from analysis.contribution import (
    calculate_contribution_data_for_categories, get_latest_contribution_summary,
    filter_contribution_by_categories, validate_contribution_data
)


def configure_page():
    """ページ設定"""
    st.set_page_config(
        page_title=DEFAULT_SETTINGS["page_title"],
        page_icon=DEFAULT_SETTINGS["page_icon"],
        layout=DEFAULT_SETTINGS["layout"]
    )


def initialize_session():
    """セッション初期化"""
    if 'snowflake_available' not in st.session_state:
        st.session_state['snowflake_available'] = False
    
    # Snowflakeセッション確認
    session = get_snowflake_session()
    return session is not None


def render_sidebar():
    """サイドバーUI"""
    st.sidebar.header("📊 CPI Analytics System")
    st.sidebar.markdown("**消費者物価指数 専門分析**")
    
    # 分析タイプ選択
    analysis_type = st.sidebar.selectbox(
        "🔍 分析タイプ",
        ["📈 主要カテゴリー分析", "🔬 個別商品分析", "📊 寄与度分析"],
        key="analysis_type"
    )
    
    # 期間設定
    st.sidebar.subheader("📅 分析期間設定")
    col1, col2 = st.sidebar.columns(2)
    
    with col1:
        start_date = st.date_input(
            "開始日",
            value=datetime.now() - timedelta(days=730),  # 2年前
            key="start_date"
        )
    
    with col2:
        end_date = st.date_input(
            "終了日",
            value=datetime.now(),
            key="end_date"
        )
    
    # 詳細設定
    with st.sidebar.expander("⚙️ 詳細設定"):
        seasonal_adjustment = st.selectbox(
            "季節調整",
            ["季節調整済み", "季節調整なし", "両方"],
            key="seasonal_adj"
        )
        
        frequency = st.selectbox(
            "データ頻度",
            ["Monthly", "Quarterly", "Annual"],
            key="frequency"
        )
        
        ai_model = st.selectbox(
            "AI分析モデル",
            AI_SETTINGS["models"],
            index=AI_SETTINGS["models"].index(AI_SETTINGS["default_model"]),
            key="ai_model"
        )
    
    return analysis_type, start_date, end_date, seasonal_adjustment, frequency, ai_model


def render_major_category_analysis(categories_df, start_date, end_date, seasonal_adjustment, frequency, ai_model):
    """主要カテゴリー分析"""
    st.header("📈 主要カテゴリー分析")
    
    # カテゴリー選択
    selected_categories = st.multiselect(
        "分析対象カテゴリーを選択",
        list(MAJOR_CPI_CATEGORIES.keys()),
        default=list(MAJOR_CPI_CATEGORIES.keys())[:3],
        key="major_categories"
    )
    
    if not selected_categories:
        st.warning("少なくとも1つのカテゴリーを選択してください。")
        return
    
    # データ準備
    filtered_categories = filter_data_by_criteria(categories_df, seasonal_adjustment, frequency)
    available_products = filtered_categories['PRODUCT'].unique().tolist()
    selected_products = prepare_products_for_category_selection(
        MAJOR_CPI_CATEGORIES, selected_categories, available_products
    )
    
    if not selected_products:
        st.error("選択されたカテゴリーのデータが見つかりません。")
        return
    
    # 変数コード取得
    variable_codes = []
    for product in selected_products:
        product_vars = filtered_categories[filtered_categories['PRODUCT'] == product]['VARIABLE'].tolist()
        variable_codes.extend(product_vars)
    
    # データ読み込み
    with st.spinner("📊 データを読み込み中..."):
        timeseries_df = load_cpi_timeseries(variable_codes, start_date, end_date)
    
    if timeseries_df.empty:
        st.error("指定期間のデータが見つかりません。")
        return
    
    # チャート表示設定
    col1, col2 = st.columns(2)
    with col1:
        chart_type = st.selectbox("表示タイプ", ["前年同月比", "前月比"], key="major_chart_type")
    with col2:
        show_baseline = st.checkbox("All Items ベースライン表示", key="show_baseline")
    
    chart_type_key = "yoy" if chart_type == "前年同月比" else "mom"
    
    # チャート作成・表示
    with st.spinner("📈 チャートを作成中..."):
        fig = create_major_category_chart(timeseries_df, selected_categories, chart_type_key)
        if show_baseline:
            fig = add_all_items_baseline_to_chart(fig, timeseries_df)
        st.plotly_chart(fig, use_container_width=True)
    
    # データテーブル
    st.subheader("📋 主要指標サマリー")
    table_data = prepare_table_data_for_categories(selected_categories, MAJOR_CPI_CATEGORIES, timeseries_df)
    if table_data:
        st.dataframe(pd.DataFrame(table_data), use_container_width=True)
    
    # AI分析
    with st.expander("🤖 AI専門分析", expanded=True):
        for category in selected_categories:
            if category in MAJOR_CPI_CATEGORIES:
                category_products = MAJOR_CPI_CATEGORIES[category]['products']
                with st.spinner(f"{category} を分析中..."):
                    analysis = generate_category_ai_analysis(
                        timeseries_df, category, category_products, ai_model
                    )
                    formatted_analysis = format_ai_analysis_result(
                        analysis, category, MAJOR_CPI_CATEGORIES[category]['color']
                    )
                    st.markdown(formatted_analysis, unsafe_allow_html=True)


def render_individual_product_analysis(categories_df, start_date, end_date, seasonal_adjustment, frequency, ai_model):
    """個別商品分析"""
    st.header("🔬 個別商品分析")
    
    # 商品選択
    filtered_categories = filter_data_by_criteria(categories_df, seasonal_adjustment, frequency)
    available_products = sorted(filtered_categories['PRODUCT'].unique())
    
    selected_products = st.multiselect(
        "分析対象商品を選択",
        available_products,
        default=available_products[:4] if len(available_products) >= 4 else available_products,
        max_selections=8,
        key="individual_products"
    )
    
    if not selected_products:
        st.warning("少なくとも1つの商品を選択してください。")
        return
    
    # 変数コード取得
    variable_codes = []
    for product in selected_products:
        product_vars = filtered_categories[filtered_categories['PRODUCT'] == product]['VARIABLE'].tolist()
        variable_codes.extend(product_vars)
    
    # データ読み込み
    with st.spinner("📊 データを読み込み中..."):
        timeseries_df = load_cpi_timeseries(variable_codes, start_date, end_date)
    
    if timeseries_df.empty:
        st.error("指定期間のデータが見つかりません。")
        return
    
    # 表示オプション
    col1, col2, col3 = st.columns(3)
    with col1:
        chart_type = st.selectbox("表示タイプ", ["前年同月比", "前月比"], key="individual_chart_type")
    with col2:
        show_histogram = st.checkbox("ヒストグラム表示", value=True, key="show_histogram")
    with col3:
        show_breakdown = st.checkbox("月別詳細内訳", key="show_breakdown")
    
    chart_type_key = "yoy" if chart_type == "前年同月比" else "mom"
    
    # チャート表示
    if show_histogram:
        with st.spinner("📊 ヒストグラムを作成中..."):
            fig = create_stacked_histogram(timeseries_df, selected_products, chart_type_key)
            st.plotly_chart(fig, use_container_width=True)
    
    if show_breakdown:
        with st.spinner("📈 詳細分析チャートを作成中..."):
            breakdown_fig = create_monthly_yoy_breakdown_chart(timeseries_df, selected_products)
            st.plotly_chart(breakdown_fig, use_container_width=True)
    
    # インフレ指標計算
    inflation_data = {}
    for product in selected_products:
        product_data = timeseries_df[timeseries_df['PRODUCT'] == product]
        if not product_data.empty:
            inflation_data[product] = calculate_inflation_metrics(product_data, product)
    
    # インフレ率比較チャート
    if inflation_data:
        st.subheader("📊 インフレ率比較")
        inflation_fig = create_inflation_rate_chart(inflation_data)
        st.plotly_chart(inflation_fig, use_container_width=True)
    
    # データテーブル
    st.subheader("📋 詳細指標テーブル")
    table_data = prepare_table_data_for_products(selected_products, timeseries_df, inflation_data)
    if table_data:
        st.dataframe(pd.DataFrame(table_data), use_container_width=True)
    
    # AI分析
    with st.expander("🤖 AI専門分析", expanded=True):
        with st.spinner("🔍 高度なAI分析を実行中..."):
            analysis = generate_ai_analysis(selected_products, inflation_data, ai_model)
            formatted_analysis = format_ai_analysis_result(analysis)
            st.markdown(formatted_analysis, unsafe_allow_html=True)


def render_contribution_analysis(start_date, end_date, frequency, ai_model):
    """寄与度分析"""
    st.header("📊 CPI寄与度分析")
    
    # カテゴリー選択
    contribution_categories = ["⚡ Energy", "🍎 Food", "📦 Core Goods", "🏠 Core Services"]
    selected_categories = st.multiselect(
        "分析対象カテゴリーを選択",
        contribution_categories,
        default=contribution_categories,
        key="contribution_categories"
    )
    
    # 寄与度データ読み込み
    with st.spinner("📊 寄与度データを読み込み中..."):
        contribution_timeseries_df = load_cpi_timeseries_for_contribution(start_date, end_date, frequency)
    
    if contribution_timeseries_df.empty:
        st.error("寄与度分析用のデータが見つかりません。")
        return
    
    # 寄与度計算
    with st.spinner("🔢 寄与度を計算中..."):
        contribution_df = calculate_contribution_data_for_categories(
            contribution_timeseries_df, 
            selected_categories,
            user_start_date=start_date
        )
    
    # データ検証
    is_valid, message = validate_contribution_data(contribution_df)
    if not is_valid:
        st.error(f"寄与度データエラー: {message}")
        return
    
    # フィルタリング
    filtered_contribution_df = filter_contribution_by_categories(contribution_df, selected_categories)
    
    # Bloomberg風チャート
    st.subheader("📈 Bloomberg風 統合寄与度分析")
    with st.spinner("📊 統合チャートを作成中..."):
        contrib_fig = create_bloomberg_contribution_chart_integrated(
            filtered_contribution_df, selected_categories
        )
        st.plotly_chart(contrib_fig, use_container_width=True)
    
    # 最新寄与度サマリー
    st.subheader("📋 最新月寄与度サマリー")
    latest_summary = get_latest_contribution_summary(filtered_contribution_df)
    if latest_summary:
        cols = st.columns(len(latest_summary))
        for i, (category, data) in enumerate(latest_summary.items()):
            with cols[i]:
                st.metric(
                    label=category,
                    value=f"{data['contribution']:.3f}pp",
                    delta=f"YoY: {data['yoy_change']:.2f}%"
                )


def render_export_section(timeseries_df, selected_products):
    """データエクスポート"""
    if timeseries_df.empty:
        return
    
    with st.expander("💾 データエクスポート"):
        export_data, yoy_export = prepare_export_data(timeseries_df, selected_products)
        
        col1, col2 = st.columns(2)
        with col1:
            csv_data = export_data.to_csv(index=False)
            st.download_button(
                label="📊 基本データCSV",
                data=csv_data,
                file_name=f"cpi_data_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv"
            )
        
        with col2:
            if not yoy_export.empty:
                yoy_csv = yoy_export.to_csv(index=False)
                st.download_button(
                    label="📈 YoY分析CSV",
                    data=yoy_csv,
                    file_name=f"cpi_yoy_analysis_{datetime.now().strftime('%Y%m%d')}.csv",
                    mime="text/csv"
                )


def main():
    """メイン関数"""
    configure_page()
    
    # セッション初期化
    if not initialize_session():
        st.error("⚠️ Snowflakeセッションを取得できません。Snowflake環境で実行してください。")
        st.stop()
    
    # サイドバー
    analysis_type, start_date, end_date, seasonal_adjustment, frequency, ai_model = render_sidebar()
    
    # メインコンテンツ
    st.title("📊 CPI Analytics System")
    st.markdown("**Professional Consumer Price Index Analysis Platform**")
    
    # CPIカテゴリデータ読み込み
    with st.spinner("🔄 CPIカテゴリデータを読み込み中..."):
        categories_df = load_cpi_categories()
    
    if categories_df.empty:
        st.error("CPIカテゴリデータを取得できませんでした。")
        st.stop()
    
    # 分析タイプ別処理
    if analysis_type == "📈 主要カテゴリー分析":
        render_major_category_analysis(
            categories_df, start_date, end_date, seasonal_adjustment, frequency, ai_model
        )
    elif analysis_type == "🔬 個別商品分析":
        render_individual_product_analysis(
            categories_df, start_date, end_date, seasonal_adjustment, frequency, ai_model
        )
    elif analysis_type == "📊 寄与度分析":
        render_contribution_analysis(start_date, end_date, frequency, ai_model)
    
    # フッター
    st.markdown("---")
    st.markdown(
        "<div style='text-align: center; color: #666;'>"
        "© 2025 CPI Analytics System | Powered by Snowflake & Streamlit"
        "</div>",
        unsafe_allow_html=True
    )


if __name__ == "__main__":
    main()