"""
CPI Data Processor Module
CPIデータの処理、計算、変換を行う関数群
"""

import pandas as pd
import numpy as np
import streamlit as st
from ..config.settings import FREQUENCY_PERIODS


def get_periods_for_frequency(frequency):
    """頻度に応じた前年同期比計算の期間数を返す"""
    return FREQUENCY_PERIODS.get(frequency, 12)  # デフォルトは Monthly


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


def filter_data_by_criteria(categories_df, seasonal_adjustment, selected_frequency):
    """条件に基づいてデータをフィルタリング"""
    
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
    
    return filtered_categories


def prepare_products_for_category_selection(major_categories, selected_categories, available_products):
    """選択されたカテゴリーから商品リストを作成"""
    selected_products = []
    for category in selected_categories:
        if category in major_categories:
            # カテゴリーの商品が利用可能な商品リストに含まれているかチェック
            category_products = major_categories[category]['products']
            available_category_products = [p for p in category_products if p in available_products]
            selected_products.extend(available_category_products)
    return list(set(selected_products))  # 重複除去


def get_inflation_trend_status(yearly_change):
    """前年同月比に基づくインフレ評価を返す"""
    if yearly_change > 3:
        return "⬆️ 高インフレ"
    elif yearly_change > 1:
        return "📈 適度"
    elif yearly_change > 0:
        return "📊 低水準"
    else:
        return "⬇️ デフレ"


def prepare_table_data_for_categories(selected_categories, major_categories, timeseries_df):
    """主要カテゴリー用のテーブルデータを準備"""
    table_data = []
    
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
    
    return table_data


def prepare_table_data_for_products(selected_products, timeseries_df, inflation_data):
    """個別商品用のテーブルデータを準備"""
    table_data = []
    
    for product in selected_products:
        product_data = timeseries_df[timeseries_df['PRODUCT'] == product]
        if not product_data.empty:
            latest_row = product_data.sort_values('DATE').iloc[-1]
            metrics = inflation_data.get(product, {})
            
            # インフレ評価
            yearly_change = metrics.get('yearly_change', 0)
            trend_status = get_inflation_trend_status(yearly_change)
            
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
    
    return table_data


def prepare_export_data(timeseries_df, selected_products):
    """エクスポート用データを準備"""
    # 基本エクスポートデータ
    export_data = timeseries_df[['DATE', 'PRODUCT', 'VALUE', 'SEASONALLY_ADJUSTED', 'VARIABLE_NAME']]
    
    # YoY%データ準備
    yoy_export = []
    for product in selected_products:
        product_data = timeseries_df[timeseries_df['PRODUCT'] == product]
        if not product_data.empty:
            yoy_data = calculate_yoy_monthly_data(product_data)
            yoy_data['PRODUCT'] = product
            yoy_export.append(yoy_data[['DATE', 'PRODUCT', 'VALUE', 'YoY_Change', 'MoM_Change']])
    
    combined_yoy = pd.concat(yoy_export, ignore_index=True) if yoy_export else pd.DataFrame()
    
    return export_data, combined_yoy