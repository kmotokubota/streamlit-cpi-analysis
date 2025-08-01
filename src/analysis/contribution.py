"""
CPI Contribution Analysis Module
CPIの寄与度分析を行う関数群
"""

import pandas as pd
import numpy as np
import streamlit as st
from ..config.settings import CPI_CONTRIBUTION_CATEGORIES
from ..data.processor import get_periods_for_frequency


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
    
    # カテゴリー定義
    categories = CPI_CONTRIBUTION_CATEGORIES
    
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
                representative_product = None
                if category_name == "Core Services" and len(category_data[category_data['PRODUCT'] == 'Services less energy services']) > 0:
                    representative_product = category_data[category_data['PRODUCT'] == 'Services less energy services'].iloc[0]
                elif category_name == "Core Goods" and len(category_data[category_data['PRODUCT'] == 'Commodities less food and energy commodities']) > 0:
                    representative_product = category_data[category_data['PRODUCT'] == 'Commodities less food and energy commodities'].iloc[0]
                elif category_name == "Food" and len(category_data[category_data['PRODUCT'] == 'Food']) > 0:
                    representative_product = category_data[category_data['PRODUCT'] == 'Food'].iloc[0]
                elif category_name == "Energy" and len(category_data[category_data['PRODUCT'] == 'Energy']) > 0:
                    representative_product = category_data[category_data['PRODUCT'] == 'Energy'].iloc[0]
                
                if representative_product is not None:
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


def create_contribution_waterfall_data(contribution_df, selected_date):
    """特定月の寄与度ウォーターフォールデータを作成"""
    if contribution_df.empty:
        return [], []
    
    # 特定日のデータを取得
    date_data = contribution_df[contribution_df['DATE'] == selected_date]
    if date_data.empty:
        return [], []
    
    categories = ['Energy', 'Food', 'Core Goods', 'Core Services']
    contributions = []
    colors = []
    
    for cat in categories:
        cat_data = date_data[date_data['Category'] == cat]
        if not cat_data.empty:
            contrib = cat_data['Contribution'].iloc[0]
            contributions.append(contrib)
            # 寄与度の正負で色分け
            colors.append('#28a745' if contrib >= 0 else '#dc3545')
        else:
            contributions.append(0)
            colors.append('#6c757d')
    
    return contributions, colors


def get_latest_contribution_summary(contribution_df):
    """最新月の寄与度サマリーを取得"""
    if contribution_df.empty:
        return {}
    
    latest_date = contribution_df['DATE'].max()
    latest_data = contribution_df[contribution_df['DATE'] == latest_date]
    
    summary = {}
    for _, row in latest_data.iterrows():
        summary[row['Category']] = {
            'contribution': row['Contribution'],
            'yoy_change': row['YoY_Change'],
            'weight': row['Weight'],
            'color': row['Color']
        }
    
    return summary


def filter_contribution_by_categories(contribution_df, selected_categories):
    """選択されたカテゴリーに基づいて寄与度データをフィルタリング"""
    if contribution_df.empty or not selected_categories:
        return contribution_df
    
    # カテゴリーマッピング
    category_mapping = {
        '⚡ Energy': 'Energy',
        '🍎 Food': 'Food', 
        '📦 Core Goods': 'Core Goods',
        '🏠 Core Services': 'Core Services'
    }
    
    # 選択されたカテゴリーを寄与度カテゴリー名にマッピング
    mapped_categories = []
    for selected_cat in selected_categories:
        if selected_cat in category_mapping:
            mapped_categories.append(category_mapping[selected_cat])
    
    if mapped_categories:
        return contribution_df[contribution_df['Category'].isin(mapped_categories)]
    else:
        return contribution_df


def validate_contribution_data(contribution_df, min_periods=12):
    """寄与度データの有効性を検証"""
    if contribution_df.empty:
        return False, "寄与度データが空です"
    
    # 最小期間数チェック
    unique_dates = contribution_df['DATE'].nunique()
    if unique_dates < min_periods:
        return False, f"データ期間が不足しています（{unique_dates}ヶ月 < {min_periods}ヶ月）"
    
    # 必要なカラムの存在チェック
    required_columns = ['DATE', 'Category', 'Contribution', 'All_Items_YoY']
    missing_columns = [col for col in required_columns if col not in contribution_df.columns]
    if missing_columns:
        return False, f"必要なカラムが不足しています: {missing_columns}"
    
    # データの整合性チェック
    null_contribution = contribution_df['Contribution'].isnull().sum()
    if null_contribution > 0:
        return False, f"寄与度にnull値が含まれています（{null_contribution}件）"
    
    return True, "データは有効です"