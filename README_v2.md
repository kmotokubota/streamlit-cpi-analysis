# 📊 CPI Analytics System (v2.0.0)

## 🆕 **新しいモジュラー構造**

CPIアナリティクスシステムが大幅にリファクタリングされ、より保守しやすく拡張可能な構造になりました。

## 📁 プロジェクト構造

```
streamlit-cpi-analysis/
├── src/                           # 🎯 メインソースコード
│   ├── main.py                   # 📱 メインStreamlitアプリ
│   ├── config/                   # ⚙️ 設定管理
│   │   ├── __init__.py
│   │   └── settings.py          # 🔧 アプリ設定・カテゴリ定義
│   ├── data/                     # 📊 データ層
│   │   ├── __init__.py
│   │   ├── loader.py            # 🔄 データ読み込み
│   │   └── processor.py         # 🧮 データ処理・計算
│   ├── visualization/            # 📈 可視化層
│   │   ├── __init__.py
│   │   ├── charts.py            # 📊 チャート作成
│   │   └── components.py        # 🧩 UIコンポーネント
│   ├── analysis/                 # 🔬 分析層
│   │   ├── __init__.py
│   │   ├── metrics.py           # 📏 指標計算
│   │   ├── ai_analyzer.py       # 🤖 AI分析
│   │   └── contribution.py      # 📈 寄与度分析
│   └── utils/                    # 🛠️ ユーティリティ
│       ├── __init__.py
│       └── helpers.py           # 🔧 ヘルパー関数
├── tests/                        # 🧪 テストファイル
├── requirements.txt              # 📦 依存関係
└── README.md                    # 📖 ドキュメント
```

## 🚀 クイックスタート

### 1. 環境セットアップ
```bash
# 依存関係インストール
pip install -r requirements.txt

# Snowflake環境で実行
streamlit run src/main.py
```

### 2. モジュール別機能

#### 📊 データ層 (`src/data/`)
- **`loader.py`**: Snowflakeからのデータ読み込み
- **`processor.py`**: YoY計算、インフレ指標、データ変換

#### 📈 可視化層 (`src/visualization/`)
- **`charts.py`**: 
  - LinkedInスタイル ヒストグラム
  - Bloomberg風寄与度チャート
  - 月別詳細分析チャート
  - インフレ率比較チャート

#### 🔬 分析層 (`src/analysis/`)
- **`ai_analyzer.py`**: AI_AGG & Cortex分析
- **`contribution.py`**: 寄与度分析・計算
- **`metrics.py`**: 専門的指標計算

#### ⚙️ 設定層 (`src/config/`)
- **`settings.py`**: 
  - CPIカテゴリ定義
  - ウェイト設定
  - カラーパレット
  - チャート設定

## 🔧 モジュール使用例

### データ読み込み
```python
from src.data.loader import load_cpi_timeseries, get_snowflake_session
from src.data.processor import calculate_inflation_metrics

# データ読み込み
df = load_cpi_timeseries(variable_codes, start_date, end_date)

# インフレ指標計算
metrics = calculate_inflation_metrics(df, "All items")
```

### チャート作成
```python
from src.visualization.charts import create_stacked_histogram
from src.config.settings import MAJOR_CPI_CATEGORIES

# LinkedInスタイル ヒストグラム作成
fig = create_stacked_histogram(df, selected_products, "yoy")
```

### AI分析
```python
from src.analysis.ai_analyzer import generate_ai_analysis

# AI分析実行
analysis = generate_ai_analysis(products, inflation_data, "claude-3-5-sonnet")
```

### 寄与度分析
```python
from src.analysis.contribution import calculate_contribution_data_for_categories

# 寄与度計算
contribution_df = calculate_contribution_data_for_categories(df, categories)
```

## 📈 新機能・改善点

### ✨ v2.0.0の主な改善
1. **🏗️ モジュラー設計**: 機能別にファイル分割、保守性向上
2. **🔧 設定の一元化**: `src/config/settings.py`で全設定管理
3. **🎨 可視化の分離**: チャート作成ロジックを独立化
4. **🤖 AI機能強化**: 分析機能を専用モジュール化
5. **📊 寄与度分析**: 専門的な寄与度計算を独立化
6. **🧪 テスト対応**: テスト可能な構造に改善
7. **📖 ドキュメント充実**: モジュール別ドキュメント

### 🔗 インポート構造
```python
# 設定
from src.config.settings import MAJOR_CPI_CATEGORIES, AI_SETTINGS

# データ層
from src.data.loader import load_cpi_categories
from src.data.processor import calculate_yoy_monthly_data

# 可視化層
from src.visualization.charts import create_major_category_chart

# 分析層
from src.analysis.ai_analyzer import generate_ai_analysis
from src.analysis.contribution import calculate_contribution_data_for_categories
```

## 🔄 マイグレーション

### 旧構造からの移行
旧の`cpi-analytics-app.py`（2135行）から新構造への主な変更点：

1. **設定の分離**: カテゴリ定義・ウェイト → `src/config/settings.py`
2. **データ処理の分離**: 読み込み・計算 → `src/data/`
3. **チャート作成の分離**: 可視化ロジック → `src/visualization/`
4. **AI分析の分離**: AI機能 → `src/analysis/ai_analyzer.py`
5. **寄与度分析の分離**: 寄与度計算 → `src/analysis/contribution.py`

## 🛠️ 開発・保守

### 新機能追加の流れ
1. 適切なモジュールを特定（data/visualization/analysis）
2. 対応するファイルに関数を追加
3. `src/main.py`でインポート・使用
4. テストファイル作成（`tests/`）

### コード品質
- **単一責任**: 各モジュールは特定の機能に集中
- **依存関係管理**: 明確なインポート構造
- **設定の一元化**: 設定変更は`settings.py`のみ
- **再利用性**: 関数の独立性・テスト可能性

## 📚 技術仕様

### 依存関係
- **Streamlit**: Web アプリケーション
- **Plotly**: インタラクティブチャート
- **Pandas**: データ処理
- **Snowflake**: データソース & AI分析

### パフォーマンス
- **キャッシュ機能**: `@st.cache_data`による高速化
- **モジュラー読み込み**: 必要な機能のみインポート
- **効率的なSQL**: 最適化されたクエリ構造

---

## 🎯 次のステップ

### v2.1.0 計画
- [ ] ユニットテスト実装
- [ ] CI/CD パイプライン
- [ ] API ドキュメント生成
- [ ] Docker化
- [ ] パフォーマンス最適化

### 長期ロードマップ
- [ ] マイクロサービス化
- [ ] リアルタイム分析
- [ ] 機械学習予測モデル
- [ ] 国際CPI比較機能

---

**© 2025 CPI Analytics System v2.0.0 | Modular & Professional**