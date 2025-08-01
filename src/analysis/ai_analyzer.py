"""
CPI AI Analysis Module
AI を使用したCPIデータの分析機能
"""

import streamlit as st
from ..data.loader import get_snowflake_session
from ..config.settings import AI_SETTINGS


def analyze_cpi_data_with_ai_agg(df, category_name, selected_products):
    """AI_AGG関数を使用してCPIデータを分析"""
    session = get_snowflake_session()
    if session is None:
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


def generate_ai_analysis(selected_products, inflation_data, ai_model=None):
    """AI分析を生成（専門的な経済分析）"""
    session = get_snowflake_session()
    if session is None:
        return "Snowflakeセッションが利用できないため、AI分析を実行できません。"
    
    if ai_model is None:
        ai_model = AI_SETTINGS["default_model"]
    
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


def generate_category_ai_analysis(timeseries_df, category, category_products, ai_model=None):
    """カテゴリー別AI分析を生成"""
    session = get_snowflake_session()
    if session is None:
        return "Snowflakeセッションが利用できません。"
    
    if ai_model is None:
        ai_model = AI_SETTINGS["default_model"]
    
    category_data = timeseries_df[timeseries_df['PRODUCT'].isin(category_products)]
    
    if category_data.empty:
        return f"{category}のデータが見つかりませんでした。"
    
    try:
        # カテゴリー名からアイコンを除去
        clean_category_name = category.replace("📈 ", "").replace("🎯 ", "").replace("🍎 ", "").replace("⚡ ", "").replace("📦 ", "").replace("🏠 ", "")
        
        # AI_AGG分析を実行
        result = analyze_cpi_data_with_ai_agg(
            category_data, 
            clean_category_name,
            category_products
        )
        
        return result
        
    except Exception as e:
        return f"{category}のAI_AGG分析でエラーが発生しました: {str(e)}"


def generate_individual_product_analysis(timeseries_df, product, ai_model=None):
    """個別商品のAI分析を生成"""
    session = get_snowflake_session()
    if session is None:
        return "Snowflakeセッションが利用できません。"
    
    product_data = timeseries_df[timeseries_df['PRODUCT'] == product]
    
    if product_data.empty:
        return f"{product}のデータが見つかりませんでした。"
    
    try:
        result = analyze_cpi_data_with_ai_agg(
            product_data, 
            product,
            [product]
        )
        
        return result
        
    except Exception as e:
        return f"{product}のAI_AGG分析でエラーが発生しました: {str(e)}"


def validate_ai_model(model_name):
    """AIモデル名の有効性を検証"""
    valid_models = AI_SETTINGS["models"]
    return model_name in valid_models


def get_ai_analysis_prompt_template(analysis_type="general"):
    """AI分析用のプロンプトテンプレートを取得"""
    templates = {
        "general": """
        消費者物価指数（CPI）の専門的な経済分析を、エコノミストの視点で実施してください。
        
        【分析要求】
        1. 価格動向の詳細分析
        2. 主要な変化要因の特定
        3. 経済への影響評価
        4. 政策的含意
        5. 今後の見通し
        
        【出力形式】
        - 専門的で簡潔な分析（500文字以内）
        - 具体的な数値とデータに基づく根拠
        - 実用的な洞察
        """,
        
        "category": """
        特定カテゴリーのCPI動向について専門的な分析を行ってください。
        
        【重点項目】
        1. カテゴリー固有の価格動向
        2. 業界・市場要因の分析
        3. 消費者への影響
        4. 今後の予測
        
        【出力要求】
        - 150文字以内の簡潔な分析
        - 日本語のみで回答
        - エコノミスト視点での専門的分析
        """,
        
        "policy": """
        金融政策の観点からCPIデータを分析してください。
        
        【分析視点】
        1. FRBの金融政策への影響
        2. インフレターゲットとの乖離
        3. 金利政策への示唆
        4. 経済政策への提言
        
        【出力形式】
        - 政策決定者向けの分析
        - 具体的な政策提言
        - 600文字以内
        """
    }
    
    return templates.get(analysis_type, templates["general"])


def format_ai_analysis_result(analysis_text, category=None, color=None):
    """AI分析結果をフォーマット"""
    if not analysis_text or analysis_text.strip() == "":
        return "分析結果を取得できませんでした。"
    
    # HTMLフォーマット
    if category and color:
        formatted_result = f"""
        <div style='background-color: {color}15; 
                    padding: 1.5rem; 
                    border-radius: 0.5rem; 
                    border-left: 4px solid {color};
                    margin: 1rem 0;'>
            <h4 style='color: {color}; margin-top: 0;'>
                {category} - 専門分析結果
            </h4>
            <p style='white-space: pre-wrap; line-height: 1.6;'>{analysis_text}</p>
        </div>
        """
    else:
        formatted_result = f"""
        <div style='background-color: #f8f9fa; 
                    padding: 1.5rem; 
                    border-radius: 0.5rem; 
                    border-left: 4px solid #007bff;
                    margin: 1rem 0;'>
            <h4 style='color: #007bff; margin-top: 0;'>
                AI分析結果
            </h4>
            <p style='white-space: pre-wrap; line-height: 1.6;'>{analysis_text}</p>
        </div>
        """
    
    return formatted_result