# 📊 CPI Analytics System

> **消費者物価指数（Consumer Price Index）包括的分析システム**  
> エコノミスト・政策立案者・アナリスト向けの高度なCPI分析プラットフォーム  

## 🎯 概要

CPI Analytics Systemは、米国労働統計局（Bureau of Labor Statistics）のデータを活用した、消費者物価指数の詳細分析を可能にするStreamlitアプリケーション。

### 🆕 最新の追加機能

#### ヒストグラム表示
- **前年同月比ヒストグラム**: 月次YoY%変化を棒グラフで表示
- **前月比ヒストグラム**: 月次MoM%変化を棒グラフで表示
- **プロフェッショナル可視化**: エコノミストが使用する標準的な表示形式

#### 月次YoY%詳細分析
- **商品別月次内訳**: 各商品の前年同月比を個別表示
- **24ヶ月トレンド**: 過去2年間の詳細な変化パターン
- **インフレ寄与度分析**: 商品別のインフレ圧力評価

#### エコノミスト級AI分析
- **専門的分析スタイル**: 高度な経済解釈
- **政策含意**: 連邦準備制度への具体的示唆
- **根本要因分析**: 価格変動の主要ドライバー特定

## ✨ 主要機能

### 📈 高度なCPI分析機能
- **3つの表示形式**: ヒストグラム（YoY）、ヒストグラム（MoM）、従来のライン表示
- **商品・サービス別分析**: 100+ CPIカテゴリーの個別・比較分析
- **季節調整対応**: 季節調整済み・未調整データの選択可能
- **期間分析**: 月次・四半期・年次データの多期間分析
- **インフレ率計算**: 前月比・前年同月比・複数期間平均の自動計算

### 🔍 LinkedIn風プロフェッショナル可視化
- **ヒストグラム表示**: 棒グラフ形式
- **月次YoY%追跡**: 各月の前年同月比変化を詳細表示
- **色分けメトリクス**: インフレ水準に応じた自動色分け
- **専門的レイアウト**: 金融業界標準のチャートデザイン

### 🤖 AI駆動経済分析
- **Snowflake Cortex AI**: 複数のLLMモデル（Llama、Claude、Mistral）
- **エコノミスト風分析**: 専門用語を適切に使用した高度な解釈
- **政策影響予測**: 金融政策への具体的影響分析
- **数値根拠**: 具体的データに基づく分析結果

## 🗂️ データソース

### Cybersynデータベース
- **FINANCE__ECONOMICS.CYBERSYN.BUREAU_OF_LABOR_STATISTICS_PRICE_ATTRIBUTES**
  - CPI商品・サービスカテゴリ情報
  - 季節調整フラグ・データ頻度・基準期間情報
- **FINANCE__ECONOMICS.CYBERSYN.BUREAU_OF_LABOR_STATISTICS_PRICE_TIMESERIES**
  - 時系列CPI値データ
  - 1950年代からの歴史的データ

### 分析対象カテゴリ例
```
Core Categories:
- Food at home（食品）          - Energy（エネルギー）
- Housing（住宅）              - Transportation（交通）
- Medical care（医療）         - Recreation（娯楽）

Detailed Categories:
- Used cars and trucks（中古車）
- Gasoline（ガソリン）
- Shelter（住居費）
- Core CPI（コアCPI）
```

## 🚀 セットアップ

### 前提条件
- Snowflakeアカウントとアクセス権限
- CybersynのBureau of Labor Statisticsデータへのアクセス
- Streamlit in Snowflake環境

### 1. 権限設定

```sql
-- Cybersynデータへのアクセス権限
USE ROLE ACCOUNTADMIN;
GRANT USAGE ON DATABASE FINANCE__ECONOMICS TO ROLE YOUR_ROLE;
GRANT USAGE ON SCHEMA FINANCE__ECONOMICS.CYBERSYN TO ROLE YOUR_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA FINANCE__ECONOMICS.CYBERSYN TO ROLE YOUR_ROLE;

-- Cortex機能の権限
GRANT USAGE ON DATABASE SNOWFLAKE TO ROLE YOUR_ROLE;
GRANT USAGE ON SCHEMA SNOWFLAKE.CORTEX TO ROLE YOUR_ROLE;
```

### 2. アプリのデプロイ

#### Option A: ファイルアップロード
1. Snowflakeウェブインターフェースの「Streamlit」セクションにアクセス
2. 「+ Streamlit App」をクリック
3. `cpi-analytics-app.py`ファイルをアップロード
4. アプリ名を「CPI_Analytics_System」に設定

#### Option B: GitHubからのデプロイ
1. レポジトリをSnowflake Gitと連携
2. 自動デプロイメントパイプラインの設定
3. 継続的インテグレーション/デプロイメント

## 📱 使用方法

### 🆕 新機能の使用方法

#### ヒストグラム表示
```
サイドバー > チャート表示設定
├── ヒストグラム（前年同月比）← 🆕 推奨
├── ヒストグラム（前月比）    ← 🆕 短期分析用
└── 従来のライン表示          ← 従来機能
```

**📊 前年同月比ヒストグラム**: 
- 月次のYoY%変化を棒グラフで表示
- インフレトレンドの視覚的把握に最適

**📊 前月比ヒストグラム**:
- 短期的な価格変動の詳細分析
- 月次のMoM%変化を棒グラフで表示
- 季節性や一時的要因の特定に有効

#### 月別YoY%詳細分析
```
チェックボックス: "📈 月別詳細内訳表示"
- 商品選択: 5個以下に制限
- 表示期間: 過去24ヶ月
- 個別チャート: 商品ごとのサブプロット表示
```

**使用例**:
```python
# エネルギー価格の詳細分析
選択商品: ['Energy', 'Gasoline', 'Natural gas']
期間: 過去24ヶ月
表示: 月別詳細内訳ON
結果: 各エネルギー商品の月次YoY%変化を個別表示
```

### 基本的な分析ワークフロー

#### 1. 分析期間の設定
```
サイドバー > 分析設定
├── 開始日: 分析開始時点
└── 終了日: 分析終了時点（デフォルト: 現在）
```

#### 2. 商品・サービスの選択
```
季節調整の選択:
├── 季節調整済み: 季節要因を除去したデータ（推奨）
├── 季節調整なし: 生データ
└── 両方: 比較分析向け

データ頻度:
└── Monthly: 月次データ（推奨）
```

#### 3. 表示形式の選択
```
🆕 チャート表示設定:
├── ヒストグラム（前年同月比）← LinkedIn風、推奨
├── ヒストグラム（前月比）    ← 短期分析
└── 従来のライン表示          ← 長期トレンド
```

### 高度な使用例

#### パターン1: Joseph Brusuelas風 CPI分析
```python
選択商品: ['Energy', 'Gasoline', 'Used cars and trucks', 'Food at home']
期間: 過去18ヶ月
表示: ヒストグラム（前年同月比）
AI分析: Claude-3-5-Sonnet
詳細内訳: ON
目的: 「The X.X% month over month increase was caused by...」式の分析
```

#### パターン2: 金融政策分析（エコノミスト向け）
```python
# FRBの2%目標との比較分析
選択商品: ['Core CPI', 'Housing', 'Services', 'Medical care']
期間: 過去24ヶ月
表示: ヒストグラム（前年同月比）
AI分析: Claude-3-5-Sonnet
焦点: 金融政策への影響予測
```

#### パターン3: 短期変動分析（トレーダー向け）
```python
# 月次変動の詳細把握
選択商品: ['Energy', 'Transportation', 'Recreation']
期間: 過去12ヶ月
表示: ヒストグラム（前月比）
AI分析: Llama3.1-70b
焦点: 短期価格ショックの特定
```

## 📊 新機能の技術詳細

### ヒストグラム表示機能

#### YoY%ヒストグラム
```python
def create_cpi_histogram_chart(df, selected_products, chart_type="yoy"):
    """CPI推移をヒストグラム形式で作成（LinkedInスタイル）"""
    # 前年同月比計算
    yoy_data['YoY_Change'] = df['VALUE'].pct_change(periods=12) * 100
    
    # 棒グラフ作成
    fig.add_trace(go.Bar(
        x=yoy_data['DATE'],
        y=yoy_data['YoY_Change'],
        name=product,
        marker_color=colors[i % len(colors)]
    ))
```

#### 月別詳細内訳
```python
def create_monthly_yoy_breakdown_chart(df, selected_products):
    """月別YoY%内訳表示（エコノミスト向け詳細分析）"""
    # 商品ごとのサブプロット作成
    fig = make_subplots(
        rows=len(selected_products), 
        cols=1,
        subplot_titles=[f"{product} - 月別前年同月比" for product in selected_products]
    )
```

### AI分析の強化

#### エコノミスト風プロンプト
```python
prompt = f"""
消費者物価指数（CPI）の専門的な経済分析を、エコノミストの視点で実施してください。
Joseph Brusuelas氏のようなプロフェッショナルな分析スタイルで回答してください。

【分析要求】
1. 各商品・サービスの価格動向の詳細分析
2. 月次変動の主要ドライバー特定  
3. インフレーション圧力の根本要因評価
4. 連邦準備制度への政策的含意
5. 今後3-6ヶ月の見通しと主要リスクファクター

参考スタイル: "The X.X% month over month increase was caused by..."のような具体的分析
"""
```

## 🔧 技術仕様

### 新機能のパフォーマンス最適化

#### YoY計算の効率化
```python
# 最適化されたYoY計算
def calculate_yoy_monthly_data(df):
    df_sorted = df.sort_values('DATE').copy()
    # 前年同月比を効率的に計算
    df_sorted['YoY_Change'] = df_sorted['VALUE'].pct_change(periods=12) * 100
    return df_sorted
```

#### チャート描画の最適化
```python
# メモリ効率的なチャート作成
@st.cache_data(ttl=1800)  # 30分キャッシュ
def create_cpi_histogram_chart(df, selected_products, chart_type):
    # 必要なデータのみを抽出して処理
```

## 📈 実用的な使用例

### 1. LinkedIn風 経済分析投稿作成

#### シナリオ: 月次CPI発表後の分析投稿
```
目的: "US Dec 2024 CPI: The 0.4% month over month..."風の投稿作成
設定:
- 商品: Energy, Gasoline, Used cars, Food at home
- 表示: ヒストグラム（前年同月比）
- AI分析: Claude-3-5-Sonnet
- 詳細内訳: ON

結果: エコノミスト級の専門分析を取得
活用: LinkedInやレポートでの専門的分析投稿
```

### 2. FRB政策分析

#### シナリオ: 金融政策委員会資料作成
```
目的: インフレ目標2%との乖離分析
設定:
- 商品: Core CPI, Housing, Services, Medical care
- 表示: ヒストグラム（前年同月比）
- 期間: 過去24ヶ月
- AI分析: 政策含意に焦点

結果: 2%目標からの乖離度と政策提言
活用: 金融政策決定の参考資料
```

### 3. 企業価格戦略分析

#### シナリオ: 業界価格動向の把握
```
目的: 自社商品価格設定の参考分析
設定:
- 商品: 関連業界カテゴリ
- 表示: ヒストグラム（前月比）+ 詳細内訳
- 期間: 過去12ヶ月
- 焦点: 月次変動パターンの把握

結果: 業界価格トレンドと変動要因
活用: 価格戦略の意思決定支援
```

## 🔍 新機能のトラブルシューティング

### ヒストグラム表示に関する問題

#### 問題: "データが表示されない"
```
原因: YoY計算に必要な12ヶ月以上のデータが不足
解決策:
- 開始日を最低13ヶ月前に設定
- 「季節調整」を「両方」に変更
- データ頻度を「Monthly」に設定
```

#### 問題: "チャートが重い"
```
原因: 大量データでのヒストグラム描画
解決策:
- 商品選択数を5個以下に制限
- 期間を24ヶ月以内に設定
- ブラウザキャッシュをクリア
```

### 月別詳細内訳の問題

#### 問題: "詳細内訳が表示されない"
```
原因: 商品選択数が5個を超過
解決策:
- 商品選択を5個以下に制限
- 重要な商品を優先選択
- 複数回に分けて分析実行
```

## 📝 変更履歴

### Version 2.0.0 (2025-01-16) - 🆕 Major Update
- **LinkedInスタイル ヒストグラム表示**: 前年同月比・前月比の棒グラフ表示
- **月次YoY%詳細分析**: 商品別の月次前年同月比詳細表示
- **エコノミスト級AI分析**: Joseph Brusuelas氏風の専門的分析
- **プロフェッショナルUI**: 金融業界標準のチャートデザイン
- **色分けメトリクス**: インフレ水準に応じた自動色分け

### Version 1.0.0 (2025-01-15)
- 初回リリース
- 基本CPI分析機能
- AI経済分析機能
- インタラクティブ可視化
- エクスポート機能

### 今後の予定

#### Version 2.1.0 (予定)
- リアルタイムCPI速報対応
- 業界別CPI比較機能
- カスタムアラート機能
- モバイル最適化

#### Version 3.0.0 (長期)
- 国際CPI比較機能
- 機械学習予測モデル
- API連携機能
- 高度なダッシュボードカスタマイズ

---

**© 2025 CPI Analytics System. All rights reserved.**

*投資判断等には必ず専門家にご相談ください。* 
