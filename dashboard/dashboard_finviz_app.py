#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Active ETF Portfolio Dashboard - Multi-Period Analysis
기간별 섹터 성과 분석 및 포트폴리오 관리
- 5일 (1주일), 10일 (2주일), 20일 (1개월) 분석
- 월간 비교 (이전 월 vs 현재 월)
- 섹터 성과 랭킹 및 트렌딩 종목
"""

import dash
from dash import dcc, html, callback, Input, Output
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
from datetime import datetime, timedelta
import logging
import os
import requests

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# API 설정
API_BASE_URL = os.getenv('API_BASE_URL', 'http://api:8000')
API_KEY = os.getenv('API_KEY', 'dev-secret-key-12345')
API_HEADERS = {
    'X-API-Key': API_KEY,
    'Content-Type': 'application/json'
}

# 섹터 ETF 매핑
SECTOR_ETF_MAP = {
    'Technology': 'QQQ',
    'Financial': 'XLF',
    'Healthcare': 'XLV',
    'Consumer Cyclical': 'XLY',
    'Communication': 'XLC',
    'Industrial': 'XLI',
    'Utilities': 'XLU',
    'Consumer Defensive': 'XLP',
    'Basic Materials': 'XLB',
    'Real Estate': 'XLRE'
}

# 벤치마크 ETF (추가 수집 대상)
BENCHMARK_ETFS = {
    'S&P 500': 'SPY',
    'Russell 2000': 'IWM', 
    'Dow Jones': 'DIA',
    'Korea': 'EWY',
    'Dividend': 'SCHD',
    'NASDAQ': 'QQQ'  # QQQ는 섹터이면서 벤치마크로도 사용
}

# 전체 ETF 리스트 (섹터 + 벤치마크, 중복 제거)
ALL_ETFS = {**SECTOR_ETF_MAP, **BENCHMARK_ETFS}
UNIQUE_ETF_TICKERS = list(set(list(SECTOR_ETF_MAP.values()) + list(BENCHMARK_ETFS.values())))


def api_request(endpoint: str, params: dict = None):
    """API 요청 헬퍼 함수"""
    try:
        url = f"{API_BASE_URL}{endpoint}"
        logger.info(f"📡 API Request: {url} with params: {params}")
        
        response = requests.get(url, headers=API_HEADERS, params=params, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        logger.info(f"✅ API Response received")
        return data
        
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ API Error: {endpoint} - {e}")
        return None


# Dash App 초기화
app = dash.Dash(
    __name__, 
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    suppress_callback_exceptions=True
)

# 메인 레이아웃
app.layout = dbc.Container([
    # 헤더
    dbc.Row([
        dbc.Col([
            html.H1("📊 Active ETF Portfolio Dashboard", 
                   className="text-center mb-2", 
                   style={'color': '#2c3e50', 'fontWeight': 'bold'}),
            html.P("💡 트렌딩 섹터 기반 포트폴리오 자동 배분 | ⏰ 5-Stage Daily Pipeline (Mon-Fri)",
                   className="text-center mb-3",
                   style={'fontSize': '14px', 'color': '#7f8c8d'}),
        ])
    ], className="mb-3"),
    
    # 기간 선택 탭
    dbc.Row([
        dbc.Col([
            dcc.Tabs(id='period-tabs', value='20d', children=[
                dcc.Tab(label='📅 5일 (1주일)', value='5d', 
                       style={'fontWeight': 'bold', 'fontSize': '14px'},
                       selected_style={'fontWeight': 'bold', 'fontSize': '14px', 'backgroundColor': '#3498db', 'color': 'white'}),
                dcc.Tab(label='📅 10일 (2주일)', value='10d', 
                       style={'fontWeight': 'bold', 'fontSize': '14px'},
                       selected_style={'fontWeight': 'bold', 'fontSize': '14px', 'backgroundColor': '#3498db', 'color': 'white'}),
                dcc.Tab(label='📅 20일 (1개월)', value='20d', 
                       style={'fontWeight': 'bold', 'fontSize': '14px'},
                       selected_style={'fontWeight': 'bold', 'fontSize': '14px', 'backgroundColor': '#3498db', 'color': 'white'}),
                dcc.Tab(label='🌙 월간 비교', value='monthly', 
                       style={'fontWeight': 'bold', 'fontSize': '14px'},
                       selected_style={'fontWeight': 'bold', 'fontSize': '14px', 'backgroundColor': '#9b59b6', 'color': 'white'}),
            ])
        ])
    ], className="mb-4"),
    
    # 기간 설명
    dbc.Row([
        dbc.Col([
            html.Div(id='period-description', className="alert alert-info text-center")
        ])
    ], className="mb-3"),
    
    # 메인 컨텐츠 영역 (탭별로 다른 내용 표시)
    html.Div(id='main-content'),
    
    # 자동 갱신 (5분)
    dcc.Interval(
        id='interval-component',
        interval=5*60*1000,  # 5분
        n_intervals=0
    )
    
], fluid=True, style={'padding': '20px'})


@callback(
    Output('period-description', 'children'),
    Input('period-tabs', 'value')
)
def update_period_description(period):
    """기간별 설명 업데이트"""
    descriptions = {
        '5d': "⚡ 5일 (1주일): 빠른 시장 변화에 민감하게 반응 | 고위험·고수익 전략",
        '10d': "⚖️ 10일 (2주일): 단기와 장기의 균형 잡힌 접근 | 중위험·중수익 전략",
        '20d': "🛡️ 20일 (1개월): 안정적이고 신뢰도 높은 트렌드 추종 | 저위험·안정수익 전략",
        'monthly': "🌙 월간 비교: 기존 월 (20일) vs 현재 월 (20일) 성과 비교 분석"
    }
    return descriptions.get(period, "")


@callback(
    Output('main-content', 'children'),
    [Input('period-tabs', 'value'),
     Input('interval-component', 'n_intervals')]
)
def update_main_content(period, n):
    """메인 컨텐츠 업데이트 (탭별 다른 레이아웃)"""
    
    if period == 'monthly':
        # 월간 비교 레이아웃
        return create_monthly_comparison_layout()
    else:
        # 기간별 분석 레이아웃 (5d, 10d, 20d)
        period_days = int(period[:-1])  # '5d' -> 5
        return create_period_analysis_layout(period_days)


def create_period_analysis_layout(period_days):
    """기간별 분석 레이아웃 생성 (5d, 10d, 20d)"""
    
    return html.Div([
        # Row 1: 섹터 성과 테이블
        dbc.Row([
            dbc.Col([
                html.H3(f"📊 {period_days}일 섹터 성과 (10개 섹터)", 
                       className="mb-3", 
                       style={'color': '#34495e', 'fontWeight': 'bold'}),
                html.Div(id=f'sector-performance-table-{period_days}d')
            ], width=12)
        ], className="mb-4"),
        
        # Row 2: 트렌딩 섹터 종목 (상위 2개 섹터에서 각각 Top 2)
        dbc.Row([
            dbc.Col([
                html.H3(f"🔥 트렌딩 섹터 종목 ({period_days}일 기준, 상위 2개 섹터)", 
                       className="mb-3", 
                       style={'color': '#34495e', 'fontWeight': 'bold'}),
                html.Div(id=f'trending-sector-stocks-{period_days}d')
            ], width=12)
        ], className="mb-4"),
        
        # Row 3: Active ETF Top 10 (월간 포트폴리오 - 20개 중 상위 10개)
        dbc.Row([
            dbc.Col([
                html.H3(f"💎 Active ETF Top 10 (월간 리스트 중 상위, 동일 비중)", 
                       className="mb-3", 
                       style={'color': '#34495e', 'fontWeight': 'bold'}),
                html.Div(id=f'active-etf-top10-{period_days}d')
            ], width=12)
        ], className="mb-4"),
        
        # Row 4: 벤치마크 vs Active Holdings 차트
        dbc.Row([
            dbc.Col([
                html.H3(f"📈 벤치마크 vs Active Holdings ({period_days}일 성과)", 
                       className="mb-3", 
                       style={'color': '#34495e', 'fontWeight': 'bold'}),
                dcc.Graph(id=f'benchmark-comparison-{period_days}d')
            ], width=12)
        ], className="mb-4"),
    ])


def create_monthly_comparison_layout():
    """월간 비교 레이아웃 생성 (기존 월 vs 현재 월)"""
    
    return html.Div([
        # Row 1: 월간 비교 테이블
        dbc.Row([
            dbc.Col([
                html.H3("🌙 월간 비교: 이전 월 (20일) vs 현재 월 (20일)", 
                       className="mb-3", 
                       style={'color': '#34495e', 'fontWeight': 'bold'}),
                html.Div(id='monthly-comparison-table')
            ], width=12)
        ], className="mb-4"),
        
        # Row 2: 현재 월 ETFs 성과 분석
        dbc.Row([
            dbc.Col([
                html.H3("📊 현재 월 ETFs 성과 분석 (20일 기준)", 
                       className="mb-3", 
                       style={'color': '#34495e', 'fontWeight': 'bold'}),
                html.Div(id='current-month-etf-performance')
            ], width=12)
        ], className="mb-4"),
        
        # Row 3: 현재 월 포트폴리오 성과 분석 (20개 종목)
        dbc.Row([
            dbc.Col([
                html.H3("💼 현재 월 포트폴리오 성과 분석 (최종 20개 종목)", 
                       className="mb-3", 
                       style={'color': '#34495e', 'fontWeight': 'bold'}),
                html.Div(id='current-month-portfolio-performance')
            ], width=12)
        ], className="mb-4"),
    ])


# ===============================================
# Callback: 섹터 성과 테이블 (기간별)
# ===============================================
@callback(
    [Output('sector-performance-table-5d', 'children'),
     Output('sector-performance-table-10d', 'children'),
     Output('sector-performance-table-20d', 'children')],
    Input('interval-component', 'n_intervals')
)
def update_sector_performance_tables(n):
    """섹터 성과 테이블 업데이트 (5d, 10d, 20d)"""
    
    results = []
    
    for period_days in [5, 10, 20]:
        # 섹터별 수익률 계산
        sector_data = []
        
        for sector, etf_ticker in SECTOR_ETF_MAP.items():
            # ETF 데이터 가져오기
            etf_data = api_request('/dashboard/etf-benchmark', 
                                  {'ticker': etf_ticker, 'days': period_days})
            
            if etf_data and len(etf_data) >= 2:
                df = pd.DataFrame(etf_data)
                df = df.sort_values('trade_date')
                df['close_price'] = pd.to_numeric(df['close_price'])
                
                start_price = df['close_price'].iloc[0]
                end_price = df['close_price'].iloc[-1]
                return_pct = ((end_price - start_price) / start_price) * 100
                
                sector_data.append({
                    'sector': sector,
                    'etf': etf_ticker,
                    'return': return_pct,
                    'start_date': df['trade_date'].iloc[0],
                    'end_date': df['trade_date'].iloc[-1]
                })
        
        if sector_data:
            # 수익률 순으로 정렬
            sector_data = sorted(sector_data, key=lambda x: x['return'], reverse=True)
            
            # 테이블 생성
            table_rows = []
            for i, item in enumerate(sector_data, 1):
                return_val = item['return']
                
                # 순위에 따라 배경색 변경
                bg_color = '#d4edda' if i <= 2 else 'white'  # Top 2는 연한 녹색
                
                table_rows.append(html.Tr([
                    html.Td(str(i), style={'padding': '12px', 'textAlign': 'center', 
                                          'fontWeight': 'bold', 'fontSize': '15px',
                                          'backgroundColor': bg_color}),
                    html.Td(item['sector'], style={'padding': '12px', 'fontWeight': 'bold',
                                                   'backgroundColor': bg_color}),
                    html.Td(item['etf'], style={'padding': '12px', 'textAlign': 'center',
                                                'color': '#3498db', 'fontWeight': 'bold',
                                                'backgroundColor': bg_color}),
                    html.Td(f"{return_val:+.2f}%", style={
                        'padding': '12px', 'textAlign': 'right',
                        'fontWeight': 'bold', 'fontSize': '16px',
                        'color': '#27ae60' if return_val >= 0 else '#e74c3c',
                        'backgroundColor': bg_color
                    }),
                    html.Td("✅ 트렌딩" if i <= 2 else "—", style={
                        'padding': '12px', 'textAlign': 'center',
                        'fontWeight': 'bold', 'color': '#27ae60' if i <= 2 else '#95a5a6',
                        'backgroundColor': bg_color
                    })
                ]))
            
            table = dbc.Table([
                html.Thead(html.Tr([
                    html.Th("순위", style={'backgroundColor': '#3498db', 'color': 'white', 
                                         'textAlign': 'center', 'padding': '12px'}),
                    html.Th("섹터", style={'backgroundColor': '#3498db', 'color': 'white', 
                                         'padding': '12px'}),
                    html.Th("ETF", style={'backgroundColor': '#3498db', 'color': 'white', 
                                        'textAlign': 'center', 'padding': '12px'}),
                    html.Th(f"{period_days}일 수익률", style={'backgroundColor': '#3498db', 
                                                          'color': 'white', 'padding': '12px'}),
                    html.Th("상태", style={'backgroundColor': '#3498db', 'color': 'white', 
                                         'textAlign': 'center', 'padding': '12px'}),
                ])),
                html.Tbody(table_rows)
            ], bordered=True, hover=True, striped=False, className="mb-0")
            
            results.append(table)
        else:
            results.append(html.P("⏳ 데이터 로딩 중...", 
                                 className="text-warning text-center"))
    
    return results


# ===============================================
# Callback: 트렌딩 섹터 종목 (기간별)
# ===============================================
@callback(
    [Output('trending-sector-stocks-5d', 'children'),
     Output('trending-sector-stocks-10d', 'children'),
     Output('trending-sector-stocks-20d', 'children')],
    Input('interval-component', 'n_intervals')
)
def update_trending_sector_stocks(n):
    """트렌딩 섹터 Top 2 종목 표시 (각 기간별)"""
    
    results = []
    
    for period_days in [5, 10, 20]:
        # 섹터별 Top 2 찾기
        sector_returns = []
        
        for sector, etf_ticker in SECTOR_ETF_MAP.items():
            etf_data = api_request('/dashboard/etf-benchmark', 
                                  {'ticker': etf_ticker, 'days': period_days})
            
            if etf_data and len(etf_data) >= 2:
                df = pd.DataFrame(etf_data)
                df = df.sort_values('trade_date')
                df['close_price'] = pd.to_numeric(df['close_price'])
                
                start_price = df['close_price'].iloc[0]
                end_price = df['close_price'].iloc[-1]
                return_pct = ((end_price - start_price) / start_price) * 100
                
                sector_returns.append({
                    'sector': sector,
                    'return': return_pct
                })
        
        # Top 2 섹터 선정
        sector_returns = sorted(sector_returns, key=lambda x: x['return'], reverse=True)
        top_2_sectors = sector_returns[:2]
        
        # 각 섹터에서 Top 2 종목 찾기
        all_stocks = []
        
        for sector_info in top_2_sectors:
            sector = sector_info['sector']
            sector_return = sector_info['return']
            
            # 해당 섹터의 종목 가져오기
            stocks_data = api_request('/dashboard/top-performers', 
                                     {'limit': 50, 'window_days': period_days})
            
            if stocks_data:
                # 섹터 필터링
                sector_stocks = [s for s in stocks_data if s.get('sector') == sector]
                top_2 = sector_stocks[:2]
                
                for stock in top_2:
                    all_stocks.append({
                        'sector': sector,
                        'sector_return': sector_return,
                        'ticker': stock.get('ticker'),
                        'company_name': stock.get('company_name', ''),
                        'return_pct': stock.get('return_pct', 0)
                    })
        
        if all_stocks:
            # 카드 형식으로 표시
            cards = []
            
            current_sector = None
            sector_cards = []
            
            for stock in all_stocks:
                if current_sector != stock['sector']:
                    if sector_cards:
                        # 이전 섹터 카드 추가
                        cards.append(dbc.Card([
                            dbc.CardHeader(html.H5(f"🏆 {current_sector} (섹터 수익률: {prev_sector_return:+.2f}%)", 
                                                  className="mb-0", style={'color': '#2c3e50'})),
                            dbc.CardBody(dbc.Row(sector_cards))
                        ], className="mb-3"))
                        sector_cards = []
                    
                    current_sector = stock['sector']
                    prev_sector_return = stock['sector_return']
                
                # 종목 카드
                stock_card = dbc.Col([
                    dbc.Card([
                        dbc.CardBody([
                            html.H5(stock['ticker'], className="card-title text-primary mb-2"),
                            html.P(stock['company_name'][:30], className="card-text mb-2", 
                                  style={'fontSize': '13px', 'color': '#7f8c8d'}),
                            html.H4(f"{stock['return_pct']:+.2f}%", 
                                   style={'color': '#27ae60' if stock['return_pct'] >= 0 else '#e74c3c',
                                         'fontWeight': 'bold'})
                        ])
                    ], color="light", outline=True)
                ], width=6, className="mb-2")
                
                sector_cards.append(stock_card)
            
            # 마지막 섹터 카드 추가
            if sector_cards:
                cards.append(dbc.Card([
                    dbc.CardHeader(html.H5(f"🏆 {current_sector} (섹터 수익률: {prev_sector_return:+.2f}%)", 
                                          className="mb-0", style={'color': '#2c3e50'})),
                    dbc.CardBody(dbc.Row(sector_cards))
                ], className="mb-3"))
            
            results.append(html.Div(cards))
        else:
            results.append(html.P("⏳ 데이터 로딩 중...", 
                                 className="text-warning text-center"))
    
    return results


# ===============================================
# Callback: Active ETF Top 10 (기간별)
# ===============================================
@callback(
    [Output('active-etf-top10-5d', 'children'),
     Output('active-etf-top10-10d', 'children'),
     Output('active-etf-top10-20d', 'children')],
    Input('interval-component', 'n_intervals')
)
def update_active_etf_top10(n):
    """Active ETF Top 10 표시 (월간 포트폴리오 중 상위 10개, 동일 비중)"""
    
    results = []
    
    for period_days in [5, 10, 20]:
        # 월간 포트폴리오 데이터 가져오기
        monthly_data = api_request('/stocks/monthly-portfolio')
        
        if monthly_data and monthly_data.get('data'):
            top_10 = monthly_data['data'][:10]
            
            # 테이블 생성 (동일 비중 10%)
            table_rows = []
            equal_weight = 10.0  # 동일 비중 10%
            
            for i, stock in enumerate(top_10, 1):
                ticker = stock.get('ticker', '')
                company_name = stock.get('company_name', '')
                return_pct = stock.get('return_pct', 0)
                
                table_rows.append(html.Tr([
                    html.Td(str(i), style={'padding': '12px', 'textAlign': 'center', 
                                          'fontWeight': 'bold', 'fontSize': '15px'}),
                    html.Td(ticker, style={'padding': '12px', 'fontWeight': 'bold', 
                                          'fontSize': '15px', 'color': '#3498db'}),
                    html.Td(company_name[:35], style={'padding': '12px', 'fontSize': '13px'}),
                    html.Td(f"{equal_weight:.1f}%", style={
                        'padding': '12px', 'textAlign': 'right',
                        'fontWeight': 'bold', 'fontSize': '16px',
                        'color': '#27ae60'
                    }),
                    html.Td(f"{return_pct:+.2f}%", style={
                        'padding': '12px', 'textAlign': 'right',
                        'fontWeight': 'bold', 'fontSize': '15px',
                        'color': '#27ae60' if return_pct >= 0 else '#e74c3c'
                    })
                ]))
            
            table = dbc.Table([
                html.Thead(html.Tr([
                    html.Th("순위", style={'backgroundColor': '#9b59b6', 'color': 'white', 
                                         'textAlign': 'center', 'padding': '12px'}),
                    html.Th("종목", style={'backgroundColor': '#9b59b6', 'color': 'white', 
                                         'padding': '12px'}),
                    html.Th("회사명", style={'backgroundColor': '#9b59b6', 'color': 'white', 
                                          'padding': '12px'}),
                    html.Th("비중", style={'backgroundColor': '#9b59b6', 'color': 'white', 
                                        'padding': '12px'}),
                    html.Th(f"{period_days}일 수익률", style={'backgroundColor': '#9b59b6', 
                                                          'color': 'white', 'padding': '12px'}),
                ])),
                html.Tbody(table_rows)
            ], bordered=True, hover=True, className="mb-2")
            
            footer = html.P(f"💡 총 10개 종목 | 동일 비중 (각 10%) | 최종 리스트는 20개 종목", 
                           className="text-muted text-center mt-2",
                           style={'fontSize': '13px', 'fontStyle': 'italic'})
            
            results.append(html.Div([table, footer]))
        else:
            results.append(html.P("⏳ 월간 포트폴리오 데이터 로딩 중...", 
                                 className="text-warning text-center"))
    
    return results


# ===============================================
# Callback: 벤치마크 비교 차트 (기간별)
# ===============================================
@callback(
    [Output('benchmark-comparison-5d', 'children'),
     Output('benchmark-comparison-10d', 'children'),
     Output('benchmark-comparison-20d', 'children')],
    Input('interval-component', 'n_intervals')
)
def update_benchmark_comparison(n):
    """벤치마크 vs Active Holdings 차트 (기간별)"""
    
    results = []
    
    for period_days in [5, 10, 20]:
        # SPY 벤치마크 데이터
        spy_data = api_request('/dashboard/spy-benchmark', {'days': period_days})
        
        # Active Holdings 데이터 (Top 10)
        monthly_data = api_request('/stocks/monthly-portfolio')
        
        fig = go.Figure()
        
        # SPY 추가
        if spy_data:
            spy_df = pd.DataFrame(spy_data)
            spy_df['trade_date'] = pd.to_datetime(spy_df['trade_date'])
            spy_df = spy_df.sort_values('trade_date')
            spy_df['close_price'] = pd.to_numeric(spy_df['close_price'])
            
            if not spy_df.empty:
                base_price = spy_df['close_price'].iloc[0]
                spy_df['cum_return'] = (spy_df['close_price'] / base_price - 1) * 100
                
                fig.add_trace(go.Scatter(
                    x=spy_df['trade_date'],
                    y=spy_df['cum_return'],
                    mode='lines',
                    name='SPY (Benchmark)',
                    line=dict(color='#3498db', width=3)
                ))
        
        # Active Portfolio 추가
        if monthly_data and monthly_data.get('data'):
            top_10_tickers = [s['ticker'] for s in monthly_data['data'][:10]]
            
            # 각 종목의 데이터 가져오기
            all_holdings = []
            for ticker in top_10_tickers:
                holdings = api_request('/dashboard/etf-holdings', 
                                      {'ticker': ticker, 'days': period_days})
                if holdings:
                    all_holdings.extend(holdings)
            
            if all_holdings:
                holdings_df = pd.DataFrame(all_holdings)
                holdings_df['trade_date'] = pd.to_datetime(holdings_df['trade_date'])
                holdings_df['close_price'] = pd.to_numeric(holdings_df['close_price'], errors='coerce')
                holdings_df = holdings_df.dropna(subset=['close_price'])
                
                if not holdings_df.empty:
                    # 피벗: 날짜별 종목 가격
                    pivot = holdings_df.pivot_table(
                        index='trade_date',
                        columns='ticker',
                        values='close_price',
                        aggfunc='last'
                    )
                    
                    if not pivot.empty:
                        # 수익률 계산
                        base_prices = pivot.iloc[0]
                        cumulative_returns = (pivot / base_prices - 1)
                        portfolio_returns = cumulative_returns.mean(axis=1, skipna=True)
                        
                        fig.add_trace(go.Scatter(
                            x=portfolio_returns.index,
                            y=portfolio_returns.values * 100,
                            mode='lines',
                            name='Active Portfolio (Top 10)',
                            line=dict(color='#27ae60', width=3)
                        ))
        
        fig.update_layout(
            xaxis_title="날짜",
            yaxis_title="누적 수익률 (%)",
            height=400,
            hovermode='x unified',
            plot_bgcolor='#f8f9fa',
            margin=dict(l=50, r=50, t=30, b=50),
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            )
        )
        
        results.append(dcc.Graph(figure=fig))
    
    return results


# ===============================================
# Callback: 월간 비교 테이블
# ===============================================
@callback(
    Output('monthly-comparison-table', 'children'),
    Input('interval-component', 'n_intervals')
)
def update_monthly_comparison(n):
    """월간 비교: 이전 월 vs 현재 월 (20일 기준)"""
    
    # 현재 월과 이전 월 계산
    today = datetime.now()
    
    # 현재 월의 20일 데이터
    current_month_end = today
    current_month_start = current_month_end - timedelta(days=20)
    
    # 이전 월의 20일 데이터 (40일 전 ~ 20일 전)
    prev_month_end = current_month_start
    prev_month_start = prev_month_end - timedelta(days=20)
    
    # 모든 ETF 비교 (섹터 + 벤치마크)
    comparison_data = []
    
    # 섹터 ETF
    for sector, etf_ticker in SECTOR_ETF_MAP.items():
        # 현재 월 데이터
        current_data = api_request('/dashboard/etf-benchmark', 
                                   {'ticker': etf_ticker, 'days': 20})
        
        # 이전 월 데이터 (40일 데이터를 가져와서 20~40일 구간 사용)
        prev_data = api_request('/dashboard/etf-benchmark', 
                               {'ticker': etf_ticker, 'days': 40})
        
        current_return = 0
        prev_return = 0
        
        if current_data and len(current_data) >= 2:
            df = pd.DataFrame(current_data)
            df = df.sort_values('trade_date')
            df['close_price'] = pd.to_numeric(df['close_price'])
            
            start_price = df['close_price'].iloc[0]
            end_price = df['close_price'].iloc[-1]
            current_return = ((end_price - start_price) / start_price) * 100
        
        if prev_data and len(prev_data) >= 30:
            df = pd.DataFrame(prev_data)
            df = df.sort_values('trade_date')
            df['close_price'] = pd.to_numeric(df['close_price'])
            
            prev_df = df.iloc[20:40] if len(df) >= 40 else df.iloc[:20]
            
            if len(prev_df) >= 2:
                start_price = prev_df['close_price'].iloc[0]
                end_price = prev_df['close_price'].iloc[-1]
                prev_return = ((end_price - start_price) / start_price) * 100
        
        change = current_return - prev_return
        
        comparison_data.append({
            'category': '섹터',
            'name': sector,
            'etf': etf_ticker,
            'prev_return': prev_return,
            'current_return': current_return,
            'change': change
        })
    
    # 벤치마크 ETF 추가
    for benchmark, etf_ticker in BENCHMARK_ETFS.items():
        # QQQ는 이미 섹터에 있으므로 스킵
        if etf_ticker in SECTOR_ETF_MAP.values():
            continue
            
        current_data = api_request('/dashboard/etf-benchmark', 
                                   {'ticker': etf_ticker, 'days': 20})
        prev_data = api_request('/dashboard/etf-benchmark', 
                               {'ticker': etf_ticker, 'days': 40})
        
        current_return = 0
        prev_return = 0
        
        if current_data and len(current_data) >= 2:
            df = pd.DataFrame(current_data)
            df = df.sort_values('trade_date')
            df['close_price'] = pd.to_numeric(df['close_price'])
            
            start_price = df['close_price'].iloc[0]
            end_price = df['close_price'].iloc[-1]
            current_return = ((end_price - start_price) / start_price) * 100
        
        if prev_data and len(prev_data) >= 30:
            df = pd.DataFrame(prev_data)
            df = df.sort_values('trade_date')
            df['close_price'] = pd.to_numeric(df['close_price'])
            
            prev_df = df.iloc[20:40] if len(df) >= 40 else df.iloc[:20]
            
            if len(prev_df) >= 2:
                start_price = prev_df['close_price'].iloc[0]
                end_price = prev_df['close_price'].iloc[-1]
                prev_return = ((end_price - start_price) / start_price) * 100
        
        change = current_return - prev_return
        
        comparison_data.append({
            'category': '벤치마크',
            'name': benchmark,
            'etf': etf_ticker,
            'prev_return': prev_return,
            'current_return': current_return,
            'change': change
        })
        
        if prev_data and len(prev_data) >= 30:  # 최소 30일 이상 필요
            df = pd.DataFrame(prev_data)
            df = df.sort_values('trade_date')
            df['close_price'] = pd.to_numeric(df['close_price'])
            
            # 20~40일 구간 (이전 월)
            prev_df = df.iloc[20:40] if len(df) >= 40 else df.iloc[:20]
            
            if len(prev_df) >= 2:
                start_price = prev_df['close_price'].iloc[0]
                end_price = prev_df['close_price'].iloc[-1]
                prev_return = ((end_price - start_price) / start_price) * 100
        
        # 변화량 계산
        change = current_return - prev_return
        
        comparison_data.append({
            'sector': sector,
            'etf': etf_ticker,
            'prev_return': prev_return,
            'current_return': current_return,
            'change': change
        })
    
    # 변화량 순으로 정렬
    comparison_data = sorted(comparison_data, key=lambda x: x['change'], reverse=True)
    
    # 테이블 생성
    table_rows = []

    for i, item in enumerate(comparison_data, 1):
        prev_ret = item['prev_return']
        curr_ret = item['current_return']
        change = item['change']
        category = item['category']

        # 카테고리에 따라 배경색 결정
        bg_color = '#fff3cd' if category == '벤치마크' else 'white'

        # 변화량에 따라 색상/아이콘 결정
        if change > 0:
            change_color = '#27ae60'
            change_icon = "📈"
        elif change < 0:
            change_color = '#e74c3c'
            change_icon = "📉"
        else:
            change_color = '#95a5a6'
            change_icon = "—"

        table_rows.append(html.Tr([
            html.Td(str(i), style={'padding': '12px', 'textAlign': 'center',
                                  'fontWeight': 'bold', 'fontSize': '15px',
                                  'backgroundColor': bg_color}),
            html.Td(category, style={'padding': '12px', 'fontSize': '13px',
                                    'color': '#e67e22' if category == '벤치마크' else '#34495e',
                                    'fontWeight': 'bold',
                                    'backgroundColor': bg_color}),
            html.Td(item['name'], style={'padding': '12px', 'fontWeight': 'bold',
                                        'backgroundColor': bg_color}),
            html.Td(item['etf'], style={'padding': '12px', 'textAlign': 'center',
                                       'color': '#3498db', 'fontWeight': 'bold',
                                       'backgroundColor': bg_color}),
            html.Td(f"{prev_ret:+.2f}%", style={
                'padding': '12px', 'textAlign': 'right',
                'color': '#95a5a6', 'fontSize': '14px',
                'backgroundColor': bg_color
            }),
            html.Td(f"{curr_ret:+.2f}%", style={
                'padding': '12px', 'textAlign': 'right',
                'fontWeight': 'bold', 'fontSize': '15px',
                'color': '#27ae60' if curr_ret >= 0 else '#e74c3c',
                'backgroundColor': bg_color
            }),
            html.Td(f"{change_icon} {change:+.2f}%", style={
                'padding': '12px', 'textAlign': 'right',
                'fontWeight': 'bold', 'fontSize': '16px',
                'color': change_color,
                'backgroundColor': bg_color
            })
        ]))

    table = dbc.Table([
        html.Thead(html.Tr([
            html.Th("순위", style={'backgroundColor': '#9b59b6', 'color': 'white',
                                 'textAlign': 'center', 'padding': '12px'}),
            html.Th("섹터", style={'backgroundColor': '#9b59b6', 'color': 'white',
                                 'padding': '12px'}),
            html.Th("ETF", style={'backgroundColor': '#9b59b6', 'color': 'white',
                                'textAlign': 'center', 'padding': '12px'}),
            html.Th("이전 월 (20일)", style={'backgroundColor': '#9b59b6',
                                          'color': 'white', 'padding': '12px'}),
            html.Th("현재 월 (20일)", style={'backgroundColor': '#9b59b6',
                                          'color': 'white', 'padding': '12px'}),
            html.Th("변화량", style={'backgroundColor': '#9b59b6', 'color': 'white',
                                  'padding': '12px'}),
        ])),
        html.Tbody(table_rows)
    ], bordered=True, hover=True, striped=True, className="mb-3")

    footer = html.P(
        f"💡 이전 월: {prev_month_start.strftime('%Y-%m-%d')} ~ {prev_month_end.strftime('%Y-%m-%d')} | "
        f"현재 월: {current_month_start.strftime('%Y-%m-%d')} ~ {current_month_end.strftime('%Y-%m-%d')}",
        className="text-muted text-center mt-2",
        style={'fontSize': '13px', 'fontStyle': 'italic'}
    )

    return html.Div([table, footer])


# ===============================================
# Callback: 현재 월 ETFs 성과 분석
# ===============================================
@callback(
    Output('current-month-etf-performance', 'children'),
    Input('interval-component', 'n_intervals')
)
def update_current_month_etf_performance(n):
    """현재 월 ETFs 성과 분석 (20일 기준) - 모든 ETF 포함"""
    
    # 모든 고유 ETF 티커 사용
    etf_data = []
    
    for ticker in UNIQUE_ETF_TICKERS:
        data = api_request('/dashboard/etf-benchmark', {'ticker': ticker, 'days': 20})
        
        if data and len(data) >= 2:
            df = pd.DataFrame(data)
            df = df.sort_values('trade_date')
            df['close_price'] = pd.to_numeric(df['close_price'])
            
            start_price = df['close_price'].iloc[0]
            end_price = df['close_price'].iloc[-1]
            return_pct = ((end_price - start_price) / start_price) * 100
            
            # ETF가 어느 카테고리에 속하는지 확인
            category = '벤치마크'
            etf_name = ticker
            for name, t in SECTOR_ETF_MAP.items():
                if t == ticker:
                    category = '섹터'
                    etf_name = name
                    break
            if category == '벤치마크':
                for name, t in BENCHMARK_ETFS.items():
                    if t == ticker:
                        etf_name = name
                        break
            
            etf_data.append({
                'ticker': ticker,
                'category': category,
                'name': etf_name,
                'start_price': start_price,
                'end_price': end_price,
                'return': return_pct,
                'start_date': df['trade_date'].iloc[0],
                'end_date': df['trade_date'].iloc[-1]
            })
    
    # 수익률 순으로 정렬
    etf_data = sorted(etf_data, key=lambda x: x['return'], reverse=True)
    
    # 테이블 생성
    table_rows = []
    
    for i, item in enumerate(etf_data, 1):
        return_val = item['return']
        category = item['category']
        
        # 카테고리에 따라 배경색
        bg_color = '#fff3cd' if category == '벤치마크' else 'white'
        
        table_rows.append(html.Tr([
            html.Td(str(i), style={'padding': '12px', 'textAlign': 'center', 
                                  'fontWeight': 'bold', 'fontSize': '15px',
                                  'backgroundColor': bg_color}),
            html.Td(category, style={'padding': '12px', 'fontSize': '13px',
                                    'color': '#e67e22' if category == '벤치마크' else '#34495e',
                                    'fontWeight': 'bold',
                                    'backgroundColor': bg_color}),
            html.Td(item['name'], style={'padding': '12px', 'fontWeight': 'bold',
                                        'fontSize': '14px',
                                        'backgroundColor': bg_color}),
            html.Td(item['ticker'], style={'padding': '12px', 'fontWeight': 'bold', 
                                          'fontSize': '15px', 'color': '#3498db',
                                          'backgroundColor': bg_color}),
            html.Td(f"${item['start_price']:.2f}", style={
                'padding': '12px', 'textAlign': 'right', 'fontSize': '14px',
                'backgroundColor': bg_color
            }),
            html.Td(f"${item['end_price']:.2f}", style={
                'padding': '12px', 'textAlign': 'right', 'fontSize': '14px',
                'backgroundColor': bg_color
            }),
            html.Td(f"{return_val:+.2f}%", style={
                'padding': '12px', 'textAlign': 'right',
                'fontWeight': 'bold', 'fontSize': '16px',
                'backgroundColor': bg_color,
                'color': '#27ae60' if return_val >= 0 else '#e74c3c'
            })
        ]))
    
    table = dbc.Table([
        html.Thead(html.Tr([
            html.Th("순위", style={'backgroundColor': '#16a085', 'color': 'white', 
                                 'textAlign': 'center', 'padding': '12px'}),
            html.Th("구분", style={'backgroundColor': '#16a085', 'color': 'white', 
                                 'padding': '12px'}),
            html.Th("이름", style={'backgroundColor': '#16a085', 'color': 'white', 
                                 'padding': '12px'}),
            html.Th("ETF", style={'backgroundColor': '#16a085', 'color': 'white', 
                                'padding': '12px'}),
            html.Th("시작가", style={'backgroundColor': '#16a085', 'color': 'white', 
                                  'padding': '12px'}),
            html.Th("종료가", style={'backgroundColor': '#16a085', 'color': 'white', 
                                  'padding': '12px'}),
            html.Th("20일 수익률", style={'backgroundColor': '#16a085', 'color': 'white', 
                                       'padding': '12px'}),
        ])),
        html.Tbody(table_rows)
    ], bordered=True, hover=True, className="mb-2")
    
    footer = html.P(f"💡 총 {len(etf_data)}개 ETF | 섹터 ETF + 벤치마크 ETF 모두 포함 | 벤치마크는 노란색 배경", 
                   className="text-muted text-center mt-2",
                   style={'fontSize': '13px', 'fontStyle': 'italic'})
    
    return html.Div([table, footer])


# ===============================================
# Callback: 현재 월 포트폴리오 성과 분석
# ===============================================
@callback(
    Output('current-month-portfolio-performance', 'children'),
    Input('interval-component', 'n_intervals')
)
def update_current_month_portfolio_performance(n):
    """현재 월 포트폴리오 성과 분석 (최종 20개 종목)"""
    
    # 월간 포트폴리오 데이터
    monthly_data = api_request('/stocks/monthly-portfolio')
    
    if not monthly_data or not monthly_data.get('data'):
        return html.P("⏳ 월간 포트폴리오 데이터 로딩 중...", 
                     className="text-warning text-center")
    
    # 최종 20개 종목
    portfolio = monthly_data['data'][:20]
    
    # 테이블 생성
    table_rows = []
    total_weight = 0
    
    for i, stock in enumerate(portfolio, 1):
        ticker = stock.get('ticker', '')
        company_name = stock.get('company_name', '')
        weight = stock.get('weight', 5.0)  # 기본 동일 비중 5%
        return_pct = stock.get('return_pct', 0)
        score = stock.get('score', 0)
        
        total_weight += weight
        
        # 상위 10개는 강조
        bg_color = '#d4edda' if i <= 10 else 'white'
        
        table_rows.append(html.Tr([
            html.Td(str(i), style={'padding': '12px', 'textAlign': 'center', 
                                  'fontWeight': 'bold', 'fontSize': '15px',
                                  'backgroundColor': bg_color}),
            html.Td(ticker, style={'padding': '12px', 'fontWeight': 'bold', 
                                  'fontSize': '15px', 'color': '#3498db',
                                  'backgroundColor': bg_color}),
            html.Td(company_name[:35], style={'padding': '12px', 'fontSize': '13px',
                                             'backgroundColor': bg_color}),
            html.Td(f"{weight:.2f}%", style={
                'padding': '12px', 'textAlign': 'right',
                'fontWeight': 'bold', 'fontSize': '15px',
                'color': '#27ae60',
                'backgroundColor': bg_color
            }),
            html.Td(f"{return_pct:+.2f}%", style={
                'padding': '12px', 'textAlign': 'right',
                'fontWeight': 'bold', 'fontSize': '15px',
                'color': '#27ae60' if return_pct >= 0 else '#e74c3c',
                'backgroundColor': bg_color
            }),
            html.Td(f"{score:.1f}", style={
                'padding': '12px', 'textAlign': 'center',
                'fontWeight': 'bold', 'color': '#e74c3c',
                'backgroundColor': bg_color
            })
        ]))
    
    table = dbc.Table([
        html.Thead(html.Tr([
            html.Th("순위", style={'backgroundColor': '#8e44ad', 'color': 'white', 
                                 'textAlign': 'center', 'padding': '12px'}),
            html.Th("종목", style={'backgroundColor': '#8e44ad', 'color': 'white', 
                                 'padding': '12px'}),
            html.Th("회사명", style={'backgroundColor': '#8e44ad', 'color': 'white', 
                                  'padding': '12px'}),
            html.Th("비중", style={'backgroundColor': '#8e44ad', 'color': 'white', 
                                'padding': '12px'}),
            html.Th("20일 수익률", style={'backgroundColor': '#8e44ad', 'color': 'white', 
                                       'padding': '12px'}),
            html.Th("점수", style={'backgroundColor': '#8e44ad', 'color': 'white', 
                                 'padding': '12px'}),
        ])),
        html.Tbody(table_rows)
    ], bordered=True, hover=True, className="mb-2")
    
    footer = html.P(f"💡 총 20개 종목 | 총 비중: {total_weight:.2f}% | "
                   f"상위 10개 종목은 Active ETF Top 10으로 동일 비중 (각 10%)", 
                   className="text-muted text-center mt-2",
                   style={'fontSize': '13px', 'fontStyle': 'italic'})
    
    return html.Div([table, footer])


# ===============================================
# 서버 실행
# ===============================================
if __name__ == '__main__':
    logger.info("🚀 Starting Dashboard Server on port 8050...")
    app.run_server(
        host='0.0.0.0',
        port=8050,
        debug=False,
        dev_tools_ui=False
    )
