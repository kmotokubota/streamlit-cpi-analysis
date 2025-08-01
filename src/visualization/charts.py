"""
CPI Charts Module
CPIデータの可視化・チャート作成機能
"""

import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import pandas as pd
import streamlit as st
from ..config.settings import CHART_CONFIG, COLOR_PALETTE, MAJOR_CPI_CATEGORIES
from ..data.processor import calculate_yoy_monthly_data


def create_stacked_histogram(df, selected_products, chart_type="yoy"):
    """積み上げヒストグラム（LinkedInスタイル）を作成"""
    fig = go.Figure()
    
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b', '#e377c2', '#7f7f7f']
    
    for i, product in enumerate(selected_products):
        product_data = df[df['PRODUCT'] == product]
        if not product_data.empty:
            # YoY%データ計算
            yoy_data = calculate_yoy_monthly_data(product_data)
            
            if chart_type == "yoy":
                y_values = yoy_data['YoY_Change']
                title_suffix = "前年同月比(%)"
            else:  # mom
                y_values = yoy_data['MoM_Change']
                title_suffix = "前月比(%)"
            
            fig.add_trace(go.Bar(
                x=yoy_data['DATE'],
                y=y_values,
                name=product,
                marker_color=colors[i % len(colors)],
                opacity=0.8,
                hovertemplate=(
                    f"<b>{product}</b><br>"
                    "日付: %{x}<br>"
                    f"{title_suffix}: %{{y:.2f}}%<br>"
                    "<extra></extra>"
                )
            ))
    
    # レイアウト設定
    fig.update_layout(
        title=f"CPI {title_suffix} - ヒストグラム表示",
        xaxis_title="日付",
        yaxis_title=f"変化率 (%)",
        barmode='group',
        **CHART_CONFIG,
        height=600,
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )
    
    # ゼロライン追加
    fig.add_hline(y=0, line_dash="dash", line_color="rgba(0,0,0,0.3)")
    
    return fig


def create_monthly_yoy_breakdown_chart(df, selected_products):
    """月別YoY%詳細分析チャート"""
    if len(selected_products) > 5:
        st.warning("詳細内訳表示は5商品まで選択可能です。最初の5商品を表示します。")
        selected_products = selected_products[:5]
    
    # サブプロット作成
    fig = make_subplots(
        rows=len(selected_products), 
        cols=1,
        subplot_titles=[f"{product} - 月別前年同月比" for product in selected_products],
        vertical_spacing=0.08
    )
    
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd']
    
    for i, product in enumerate(selected_products):
        product_data = df[df['PRODUCT'] == product]
        if not product_data.empty:
            yoy_data = calculate_yoy_monthly_data(product_data)
            
            # 色分け（インフレ水準に応じて）
            bar_colors = []
            for val in yoy_data['YoY_Change']:
                if pd.isna(val):
                    bar_colors.append('#cccccc')
                elif val > 3:
                    bar_colors.append('#dc3545')  # 高インフレ（赤）
                elif val > 1:
                    bar_colors.append('#fd7e14')  # 適度（オレンジ）
                elif val > 0:
                    bar_colors.append('#28a745')  # 低水準（緑）
                else:
                    bar_colors.append('#007bff')  # デフレ（青）
            
            fig.add_trace(
                go.Bar(
                    x=yoy_data['DATE'],
                    y=yoy_data['YoY_Change'],
                    name=product,
                    marker_color=bar_colors,
                    showlegend=False,
                    hovertemplate=(
                        f"<b>{product}</b><br>"
                        "日付: %{x}<br>"
                        "前年同月比: %{y:.2f}%<br>"
                        "<extra></extra>"
                    )
                ),
                row=i+1, col=1
            )
            
            # 各サブプロットにゼロライン追加
            fig.add_hline(y=0, line_dash="dash", line_color="rgba(0,0,0,0.3)", row=i+1, col=1)
    
    # レイアウト更新
    fig.update_layout(
        title="月別前年同月比詳細分析 - 色分けメトリクス",
        height=300 * len(selected_products),
        **CHART_CONFIG,
        showlegend=False
    )
    
    # X軸の設定
    for i in range(len(selected_products)):
        fig.update_xaxes(title_text="日付" if i == len(selected_products)-1 else "", row=i+1, col=1)
        fig.update_yaxes(title_text="前年同月比(%)", row=i+1, col=1)
    
    return fig


def create_major_category_chart(df, selected_categories, chart_type="yoy"):
    """主要カテゴリーチャートを作成"""
    fig = go.Figure()
    
    for category in selected_categories:
        if category in MAJOR_CPI_CATEGORIES:
            category_info = MAJOR_CPI_CATEGORIES[category]
            products = category_info['products']
            color = category_info['color']
            display_type = category_info['display_type']
            
            # カテゴリーの代表商品データを取得
            main_product = products[0]
            product_data = df[df['PRODUCT'] == main_product]
            
            if not product_data.empty:
                if chart_type == "yoy":
                    yoy_data = calculate_yoy_monthly_data(product_data)
                    y_values = yoy_data['YoY_Change']
                    title_suffix = "前年同月比"
                else:  # mom
                    yoy_data = calculate_yoy_monthly_data(product_data)
                    y_values = yoy_data['MoM_Change']
                    title_suffix = "前月比"
                
                # 表示タイプに応じてトレース追加
                if display_type == "line":
                    fig.add_trace(go.Scatter(
                        x=yoy_data['DATE'],
                        y=y_values,
                        mode='lines+markers',
                        name=category,
                        line=dict(color=color, width=3),
                        marker=dict(size=6),
                        hovertemplate=(
                            f"<b>{category}</b><br>"
                            "日付: %{x}<br>"
                            f"{title_suffix}: %{{y:.2f}}%<br>"
                            "<extra></extra>"
                        )
                    ))
                else:  # bar
                    fig.add_trace(go.Bar(
                        x=yoy_data['DATE'],
                        y=y_values,
                        name=category,
                        marker_color=color,
                        opacity=0.7,
                        hovertemplate=(
                            f"<b>{category}</b><br>"
                            "日付: %{x}<br>"
                            f"{title_suffix}: %{{y:.2f}}%<br>"
                            "<extra></extra>"
                        )
                    ))
    
    # レイアウト設定
    fig.update_layout(
        title=f"CPI主要カテゴリー{title_suffix}(%)",
        xaxis_title="日付",
        yaxis_title=f"変化率 (%)",
        **CHART_CONFIG,
        height=600,
        showlegend=True
    )
    
    # ゼロライン追加
    fig.add_hline(y=0, line_dash="dash", line_color="rgba(0,0,0,0.3)")
    
    return fig


def create_bloomberg_contribution_chart_integrated(contribution_df, selected_categories=None):
    """Bloomberg風統合寄与度チャート"""
    if contribution_df.empty:
        return go.Figure()
    
    fig = make_subplots(
        rows=2, cols=1,
        subplot_titles=("CPI前年同月比寄与度分析", "All Items CPI前年同月比推移"),
        vertical_spacing=0.12,
        row_heights=[0.7, 0.3]
    )
    
    # 寄与度積み上げ棒グラフ
    categories = contribution_df['Category'].unique()
    
    for category in categories:
        category_data = contribution_df[contribution_df['Category'] == category]
        category_color = category_data['Color'].iloc[0] if not category_data.empty else '#1f77b4'
        
        fig.add_trace(
            go.Bar(
                x=category_data['DATE'],
                y=category_data['Contribution'],
                name=f"{category}",
                marker_color=category_color,
                opacity=0.8,
                hovertemplate=(
                    f"<b>{category}</b><br>"
                    "日付: %{x}<br>"
                    "寄与度: %{y:.3f}pp<br>"
                    "<extra></extra>"
                ),
                legendgroup="contribution"
            ),
            row=1, col=1
        )
    
    # All Items CPI推移ライン（下段）
    if 'All_Items_YoY' in contribution_df.columns:
        all_items_data = contribution_df.drop_duplicates('DATE')[['DATE', 'All_Items_YoY']]
        
        fig.add_trace(
            go.Scatter(
                x=all_items_data['DATE'],
                y=all_items_data['All_Items_YoY'],
                mode='lines+markers',
                name='All Items CPI',
                line=dict(color='#000080', width=3),
                marker=dict(size=6),
                hovertemplate=(
                    "<b>All Items CPI</b><br>"
                    "日付: %{x}<br>"
                    "前年同月比: %{y:.2f}%<br>"
                    "<extra></extra>"
                ),
                legendgroup="cpi",
                showlegend=True
            ),
            row=2, col=1
        )
        
        # FRBターゲット2%ライン
        fig.add_hline(y=2, line_dash="dash", line_color="red", 
                     annotation_text="FRB Target 2%", row=2, col=1)
    
    # Core CPI推移ライン（選択されている場合）
    if 'Core_CPI_YoY' in contribution_df.columns and selected_categories and '🎯 Core CPI' in selected_categories:
        core_cpi_data = contribution_df.drop_duplicates('DATE')[['DATE', 'Core_CPI_YoY']].dropna()
        if not core_cpi_data.empty:
            fig.add_trace(
                go.Scatter(
                    x=core_cpi_data['DATE'],
                    y=core_cpi_data['Core_CPI_YoY'],
                    mode='lines+markers',
                    name='Core CPI',
                    line=dict(color='#D32F2F', width=2, dash='dot'),
                    marker=dict(size=4),
                    hovertemplate=(
                        "<b>Core CPI</b><br>"
                        "日付: %{x}<br>"
                        "前年同月比: %{y:.2f}%<br>"
                        "<extra></extra>"
                    ),
                    legendgroup="cpi"
                ),
                row=2, col=1
            )
    
    # レイアウト設定
    fig.update_layout(
        title="CPI寄与度分析 - Bloomberg風統合ダッシュボード",
        height=800,
        **CHART_CONFIG,
        barmode='relative',  # 積み上げ棒グラフ
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=-0.15,
            xanchor="center",
            x=0.5
        )
    )
    
    # 軸設定
    fig.update_xaxes(title_text="", row=1, col=1)
    fig.update_xaxes(title_text="日付", row=2, col=1)
    fig.update_yaxes(title_text="寄与度(pp)", row=1, col=1)
    fig.update_yaxes(title_text="前年同月比(%)", row=2, col=1)
    
    # ゼロライン追加
    fig.add_hline(y=0, line_dash="solid", line_color="rgba(0,0,0,0.3)", row=1, col=1)
    fig.add_hline(y=0, line_dash="solid", line_color="rgba(0,0,0,0.3)", row=2, col=1)
    
    return fig


def create_inflation_rate_chart(inflation_data):
    """インフレ率チャート作成"""
    if not inflation_data:
        return go.Figure()
    
    products = list(inflation_data.keys())
    yearly_changes = [inflation_data[p].get('yearly_change', 0) for p in products]
    monthly_changes = [inflation_data[p].get('monthly_change', 0) for p in products]
    
    # 色分け（インフレ水準に応じて）
    colors = []
    for yoy in yearly_changes:
        if yoy > 3:
            colors.append('#dc3545')  # 高インフレ（赤）
        elif yoy > 1:
            colors.append('#fd7e14')  # 適度（オレンジ）
        elif yoy > 0:
            colors.append('#28a745')  # 低水準（緑）
        else:
            colors.append('#007bff')  # デフレ（青）
    
    fig = go.Figure()
    
    # 前年同月比バー
    fig.add_trace(go.Bar(
        x=products,
        y=yearly_changes,
        name='前年同月比',
        marker_color=colors,
        opacity=0.8,
        yaxis='y',
        hovertemplate=(
            "<b>%{x}</b><br>"
            "前年同月比: %{y:.2f}%<br>"
            "<extra></extra>"
        )
    ))
    
    # 前月比ライン
    fig.add_trace(go.Scatter(
        x=products,
        y=monthly_changes,
        mode='lines+markers',
        name='前月比',
        line=dict(color='#ff7f0e', width=3),
        marker=dict(size=8),
        yaxis='y2',
        hovertemplate=(
            "<b>%{x}</b><br>"
            "前月比: %{y:.2f}%<br>"
            "<extra></extra>"
        )
    ))
    
    # レイアウト設定
    fig.update_layout(
        title="商品別インフレ率比較（前年同月比 vs 前月比）",
        xaxis_title="商品・サービス",
        yaxis=dict(
            title="前年同月比(%)",
            side="left"
        ),
        yaxis2=dict(
            title="前月比(%)",
            side="right",
            overlaying="y"
        ),
        **CHART_CONFIG,
        height=600,
        showlegend=True
    )
    
    # ゼロライン追加
    fig.add_hline(y=0, line_dash="dash", line_color="rgba(0,0,0,0.3)")
    
    return fig


def add_all_items_baseline_to_chart(fig, df):
    """チャートにAll Items CPIベースラインを追加"""
    all_items_data = df[df['PRODUCT'] == 'All items']
    if not all_items_data.empty:
        yoy_data = calculate_yoy_monthly_data(all_items_data)
        
        fig.add_trace(go.Scatter(
            x=yoy_data['DATE'],
            y=yoy_data['YoY_Change'],
            mode='lines',
            name='All Items (ベースライン)',
            line=dict(color='rgba(0,0,0,0.3)', width=2, dash='dot'),
            hovertemplate=(
                "<b>All Items (ベースライン)</b><br>"
                "日付: %{x}<br>"
                "前年同月比: %{y:.2f}%<br>"
                "<extra></extra>"
            )
        ))
    
    return fig