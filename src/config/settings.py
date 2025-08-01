"""
CPI Analytics Application Settings
主要なCPIカテゴリ、ウェイト、色設定などを定義
"""

# 主要CPIカテゴリー定義
MAJOR_CPI_CATEGORIES = {
    "📈 All Items": {
        "products": ["All items"],
        "description": "総合CPI（全項目）",
        "color": "#1E88E5",
        "display_type": "line"
    },
    "🎯 Core CPI": {
        "products": ["All items less food and energy", "All items less food, energy, and shelter"],
        "description": "コアCPI（食品・エネルギー除く）",
        "color": "#D32F2F",
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

# 寄与度分析用カテゴリーとウェイト
CPI_CONTRIBUTION_CATEGORIES = {
    "Core Services": {
        "products": ["Services less energy services", "Shelter", "Transportation services"],
        "weight": 0.58,  # 約58%
        "color": "#4E8397",
        "description": "コアサービス（住宅・交通・その他サービス）"
    },
    "Core Goods": {
        "products": ["Commodities less food and energy commodities", "New vehicles", "Used vehicles and trucks"],
        "weight": 0.20,  # 約20%
        "color": "#845EC2",
        "description": "コア商品（自動車・家具・衣料等）"
    },
    "Food": {
        "products": ["Food", "Food at home", "Food away from home"],
        "weight": 0.14,  # 約14%
        "color": "#F18F01",
        "description": "食品（内食・外食）"
    },
    "Energy": {
        "products": ["Energy", "Energy commodities", "Energy services", "Gasoline (all types)"],
        "weight": 0.08,  # 約8%
        "color": "#C73E1D",
        "description": "エネルギー（ガソリン・電気・ガス）"
    }
}

# 頻度に応じた期間数設定
FREQUENCY_PERIODS = {
    'Monthly': 12,       # 12ヶ月前
    'Quarterly': 4,      # 4四半期前
    'Semi-annual': 2,    # 2半期前
    'Annual': 1          # 1年前
}

# デフォルト設定
DEFAULT_SETTINGS = {
    "page_title": "📊 CPI Analytics System",
    "page_icon": "📊",
    "layout": "wide",
    "cache_ttl": 3600,  # 1時間
    "default_frequency": "Monthly",
    "default_seasonal_adjustment": "季節調整済み",
    "max_products": 8,
    "chart_height": 600,
    "chart_width": 1200
}

# AI分析設定
AI_SETTINGS = {
    "models": ["llama3.1-70b", "claude-3-5-sonnet", "mistral-large2"],
    "default_model": "claude-3-5-sonnet",
    "max_tokens": 1000,
    "temperature": 0.1
}

# カラーパレット
COLOR_PALETTE = {
    "primary": "#1f77b4",
    "secondary": "#ff7f0e", 
    "success": "#2ca02c",
    "danger": "#d62728",
    "warning": "#ff7f0e",
    "info": "#17a2b8",
    "light": "#f8f9fa",
    "dark": "#343a40"
}

# チャート設定
CHART_CONFIG = {
    "plot_bgcolor": "white",
    "paper_bgcolor": "white",
    "font_family": "Arial, sans-serif",
    "title_font_size": 18,
    "axis_font_size": 14,
    "legend_font_size": 12,
    "margin": dict(l=80, r=80, t=100, b=60),
    "hovermode": "x unified"
}