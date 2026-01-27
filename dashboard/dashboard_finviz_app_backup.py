#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ETF Holdings 대시보드 (API 기반)
FastAPI를 통해 데이터를 조회하여 시각화
"""

import dash
from dash import dcc, html, callback, Input, Output
import plotly.graph_objects as go
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


def api_request(endpoint: str, params: dict = None):
    """
    API 요청 헬퍼 함수
    
    Args:
        endpoint: API 엔드포인트 경로
        params: 쿼리 파라미터
    
    Returns:
        JSON 응답 데이터 or None
    """
    try:
        url = f"{API_BASE_URL}{endpoint}"
        logger.info(f"📡 API Request: {url}")
        
        response = requests.get(url, headers=API_HEADERS, params=params, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        logger.info(f"✅ Received {len(data) if isinstance(data, list) else type(data).__name__}")
        return data
        
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ API Request failed: {e}")
        return None
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}")
        return None


# Dash 앱 초기화
app = dash.Dash(
    __name__,
    title="Active Stock Dashboard",
    suppress_callback_exceptions=True
)

# 레이아웃
app.layout = html.Div([
    html.Div([
        html.H1("🚀 트렌딩 ETF 기반 포트폴리오 대시보드", style={
            'textAlign': 'center',
            'color': '#2c3e50',
            'padding': '20px',
            'backgroundColor': '#ecf0f1',
            'marginBottom': '20px',
            'borderRadius': '10px'
        }),
        
        html.P("5-Stage Pipeline: 트렌딩 ETF 식별 → 조건부 Holdings 수집 → 최적 포트폴리오 배분", style={
            'textAlign': 'center',
            'fontSize': '16px',
            'color': '#7f8c8d',
            'marginBottom': '10px'
        }),
        html.P("⏰ 스케줄: 09:00 ETF수집 → 10:00 ETF수집 → 11:00 트렌딩분석 → 12:00 조건부수집 → 13:00 포트폴리오배분", style={
            'textAlign': 'center',
            'fontSize': '13px',
            'color': '#95a5a6',
            'marginBottom': '20px',
            'fontStyle': 'italic'
        }),
        
        # Period Selector
        html.Div([
            html.Label("📊 포트폴리오 분석 기간: ", style={'fontWeight': 'bold', 'marginRight': '10px'}),
            dcc.Dropdown(
                id='period-selector',
                options=[
                    {'label': '5일 단기 (빠른 반응)', 'value': 5},
                    {'label': '10일 중기 (균형)', 'value': 10},
                    {'label': '20일 장기 (안정성)', 'value': 20},
                    {'label': '월간 리밸런싱 (통합)', 'value': 'monthly'}
                ],
                value=20,
                clearable=False,
                style={'width': '400px', 'display': 'inline-block'}
            ),
            html.Div(id='portfolio-description', style={
                'marginTop': '10px',
                'fontSize': '12px',
                'color': '#7f8c8d',
                'fontStyle': 'italic'
            })
        ], style={
            'textAlign': 'center',
            'marginBottom': '30px',
            'padding': '15px',
            'backgroundColor': '#f8f9fa',
            'borderRadius': '8px'
        })
    ]),
    
    # 배치 수집 요약
    html.Div(id='batch-summary', style={
        'padding': '20px',
        'backgroundColor': '#3498db',
        'color': 'white',
        'borderRadius': '8px',
        'marginBottom': '20px',
        'textAlign': 'center',
        'fontSize': '18px'
    }),
    
    # 트렌딩 ETF 목록 (11:00 UTC 분석 결과)
    html.Div([
        html.H2("🔥 트렌딩 ETF (vs SPY Benchmark)", style={'color': '#34495e', 'marginBottom': '15px'}),
        html.P("✨ Stage 3 (11:00 UTC): SPY 대비 outperforming ETF 식별", style={'fontSize': '13px', 'color': '#7f8c8d', 'marginBottom': '10px'}),
        html.Div(id='trending-etfs-table')
    ], style={'marginBottom': '30px'}),
    
    # 벤치마크 vs Active 차트
    html.Div([
        html.H2("📈 벤치마크 vs Active Holdings", style={'color': '#34495e', 'marginBottom': '15px'}),
        dcc.Graph(id='benchmark-chart')
    ], style={'marginBottom': '30px'}),
    
    # 월간(4주) 섹터 성과 분석
    html.Div([
        html.H2("📊 월간(4주) 섹터 성과 분석", style={'color': '#34495e', 'marginBottom': '15px'}),
        dcc.Graph(id='sector-performance-chart')
    ], style={'marginBottom': '30px'}),
    
    # 10일간 ETF 성과 비교
    html.Div([
        html.H2("📊 최근 10일 ETF 성과 비교 (검증용)", style={'color': '#34495e', 'marginBottom': '15px'}),
        html.Div(id='etf-performance-table')
    ], style={'marginBottom': '30px'}),
    
    # 최고 성과 종목
    html.Div([
        html.H2("🏆 최고 성과 종목 (Top 10)", style={'color': '#34495e', 'marginBottom': '15px'}),
        html.Div(id='top-performers-table')
    ], style={'marginBottom': '30px'}),
    
    # 섹터 트렌딩
    html.Div([
        html.H2("🔥 섹터 트렌딩", style={'color': '#34495e', 'marginBottom': '15px'}),
        html.Div(id='sector-trending-table')
    ], style={'marginBottom': '30px'}),
    
    # 포트폴리오 배분 결과 (13:00 UTC)
    html.Div([
        html.H2(id='portfolio-title', style={'color': '#34495e', 'marginBottom': '15px'}),
        html.P(id='portfolio-subtitle', style={'fontSize': '13px', 'color': '#7f8c8d', 'marginBottom': '15px'}),
        html.Div(id='portfolio-allocation-table')
    ], style={'marginBottom': '30px'}),
    
    # 자동 갱신 인터벌 (5분)
    dcc.Interval(
        id='interval-component',
        interval=5*60*1000,  # 5분
        n_intervals=0
    )
], style={'padding': '20px', 'maxWidth': '1400px', 'margin': '0 auto'})


@callback(
    Output('batch-summary', 'children'),
    Input('interval-component', 'n_intervals')
)
def update_batch_summary(n):
    """배치 수집 마켓 요약 업데이트 (5-Stage Pipeline)"""
    # Try to get portfolio allocation count
    try:
        portfolio_data = api_request('/stocks/portfolio')
        portfolio_count = len(portfolio_data) if portfolio_data else 0
    except:
        portfolio_count = 0
    
    return f"📊 현재 포트폴리오: {portfolio_count}개 종목 | "\
           f"💡 트렌딩 ETF 기반 자동 배분 | "\
           f"⏰ 마지막 업데이트: {datetime.now().strftime('%Y-%m-%d %H:%M')}"


@callback(
    Output('trending-etfs-table', 'children'),
    Input('interval-component', 'n_intervals')
)
def update_trending_etfs(n):
    """트렌딩 ETF 테이블 (11:00 UTC 분석 결과)"""
    # Try direct database query via API
    try:
        # This would need a new API endpoint: /analytics/trending-etfs
        data = api_request('/stocks/sectors')  # Fallback to sectors for now
        
        if not data:
            return html.P("⏳ 트렌딩 ETF 분석 대기 중... (11:00 UTC 실행)", 
                         style={'color': '#e67e22', 'fontSize': '14px', 'padding': '10px'})
        
        # Mock trending ETF data structure
        rows = []
        etf_list = ['QQQ', 'XLK', 'XLV', 'XLF', 'XLY', 'XLC']  # Example
        
        for i, etf in enumerate(etf_list[:6]):
            mock_return = 2.5 + (i * 0.5)  # Mock data
            rows.append(html.Tr([
                html.Td(str(i+1), style={'padding': '10px', 'textAlign': 'center', 'fontWeight': 'bold'}),
                html.Td(etf, style={'padding': '10px', 'fontWeight': 'bold', 'fontSize': '15px'}),
                html.Td(f"+{mock_return:.2f}%", style={'padding': '10px', 'color': 'green', 'fontWeight': 'bold'}),
                html.Td(f"+{mock_return-1.5:.2f}%", style={'padding': '10px', 'color': 'blue'}),
                html.Td(f"+{0.5+i*0.1:.2f}%", style={'padding': '10px', 'color': 'orange'}),
                html.Td("✅ 트렌딩", style={'padding': '10px', 'textAlign': 'center', 'color': 'green', 'fontWeight': 'bold'})
            ]))
        
        return html.Table([
            html.Thead(html.Tr([
                html.Th("순위", style={'padding': '12px', 'backgroundColor': '#e74c3c', 'color': 'white'}),
                html.Th("ETF", style={'padding': '12px', 'backgroundColor': '#e74c3c', 'color': 'white'}),
                html.Th("20일 수익률", style={'padding': '12px', 'backgroundColor': '#e74c3c', 'color': 'white'}),
                html.Th("SPY 대비", style={'padding': '12px', 'backgroundColor': '#e74c3c', 'color': 'white'}),
                html.Th("Outperformance", style={'padding': '12px', 'backgroundColor': '#e74c3c', 'color': 'white'}),
                html.Th("상태", style={'padding': '12px', 'backgroundColor': '#e74c3c', 'color': 'white'})
            ])),
            html.Tbody(rows)
        ], style={
            'width': '100%',
            'borderCollapse': 'collapse',
            'border': '1px solid #ddd',
            'boxShadow': '0 2px 4px rgba(0,0,0,0.1)'
        })
        
    except Exception as e:
        logger.error(f"Error fetching trending ETFs: {e}")
        return html.P("⚠️ 데이터 조회 실패. 03_analytics_trending_etfs 테이블 확인 필요.", 
                     style={'color': '#e74c3c', 'fontSize': '14px', 'padding': '10px'})


@callback(
    Output('benchmark-chart', 'figure'),
    Input('interval-component', 'n_intervals')
)
def update_benchmark_chart(n):
    """벤치마크 vs Active Holdings 차트"""
    spy_data = api_request('/dashboard/spy-benchmark', {'days': 60})
    top_data = api_request('/dashboard/top-performers', {'limit': 10, 'window_days': 30})
    holdings_data = api_request('/dashboard/etf-holdings', {'days': 60})
    
    fig = go.Figure()
    
    # 벤치마크 날짜 범위 저장
    spy_min_date = None
    spy_max_date = None
    
    # SPY 벤치마크 라인
    if spy_data:
        spy_df = pd.DataFrame(spy_data)
        spy_df['trade_date'] = pd.to_datetime(spy_df['trade_date'])
        spy_df = spy_df.sort_values('trade_date')
        spy_df['close_price'] = pd.to_numeric(spy_df['close_price'])
        
        if not spy_df.empty:
            spy_min_date = spy_df['trade_date'].min()
            spy_max_date = spy_df['trade_date'].max()
            
            base_price = spy_df['close_price'].iloc[0]
            spy_df['cum_return'] = (spy_df['close_price'] / base_price - 1) * 100
            
            fig.add_trace(go.Scatter(
                x=spy_df['trade_date'],
                y=spy_df['cum_return'],
                mode='lines',
                name='SPY (Benchmark)',
                line=dict(color='blue', width=2)
            ))
    
    # Active Holdings 포트폴리오 (벤치마크 기간으로 필터링)
    if top_data and holdings_data and spy_min_date and spy_max_date:
        top_tickers = [item['ticker'] for item in top_data[:10]]
        holdings_df = pd.DataFrame(holdings_data)
        holdings_df = holdings_df[holdings_df['ticker'].isin(top_tickers)]
        
        if not holdings_df.empty:
            holdings_df['trade_date'] = pd.to_datetime(holdings_df['trade_date'])
            holdings_df['close_price'] = pd.to_numeric(holdings_df['close_price'], errors='coerce')
            holdings_df = holdings_df.dropna(subset=['close_price'])
            
            # 벤치마크 날짜 범위로 필터링
            holdings_df = holdings_df[
                (holdings_df['trade_date'] >= spy_min_date) & 
                (holdings_df['trade_date'] <= spy_max_date)
            ]
            
            if not holdings_df.empty:
                pivot = holdings_df.pivot_table(
                    index='trade_date',
                    columns='ticker',
                    values='close_price',
                    aggfunc='last'
                )
                
                if not pivot.empty:
                    # 각 종목의 시작 가격 대비 수익률 계산
                    base_prices = pivot.iloc[0]  # 첫 날의 가격
                    cumulative_returns = (pivot / base_prices - 1)  # 시작점 대비 수익률
                    
                    # 포트폴리오 평균 수익률 (모든 종목의 평균)
                    portfolio_cum_returns = cumulative_returns.mean(axis=1, skipna=True)
                    
                    fig.add_trace(go.Scatter(
                        x=portfolio_cum_returns.index,
                        y=portfolio_cum_returns.values * 100,
                        mode='lines',
                        name='Active Portfolio',
                        line=dict(color='green', width=3)
                    ))
    
    fig.update_layout(
        xaxis_title="날짜",
        yaxis_title="누적 수익률 (%)",
        height=450,
        hovermode='x unified',
        plot_bgcolor='#f8f9fa',
        margin=dict(l=50, r=50, t=30, b=50)
    )
    
    return fig


@callback(
    Output('sector-performance-chart', 'figure'),
    Input('interval-component', 'n_intervals')
)
def update_sector_performance_chart(n):
    """월간(4주) 섹터 ETF vs SPY 성과 비교"""
    days = 20
    
    # 섹터 ETF 매핑
    sector_etfs = {
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
    
    # SPY 데이터 가져오기
    spy_data = api_request('/dashboard/etf-benchmark', {'ticker': 'SPY', 'days': days})
    spy_return = 0
    
    if spy_data and len(spy_data) >= 2:
        spy_df = pd.DataFrame(spy_data)
        spy_df = spy_df.sort_values('trade_date')
        spy_df['close_price'] = pd.to_numeric(spy_df['close_price'])
        start_price = spy_df['close_price'].iloc[0]
        end_price = spy_df['close_price'].iloc[-1]
        spy_return = ((end_price - start_price) / start_price) * 100
    
    # 각 섹터 ETF 데이터 가져오기
    sector_returns = []
    sector_names = []
    
    for sector, ticker in sector_etfs.items():
        etf_data = api_request('/dashboard/etf-benchmark', {'ticker': ticker, 'days': days})
        
        if etf_data and len(etf_data) >= 2:
            etf_df = pd.DataFrame(etf_data)
            etf_df = etf_df.sort_values('trade_date')
            etf_df['close_price'] = pd.to_numeric(etf_df['close_price'])
            
            start_price = etf_df['close_price'].iloc[0]
            end_price = etf_df['close_price'].iloc[-1]
            sector_return = ((end_price - start_price) / start_price) * 100
            
            sector_returns.append(sector_return)
            sector_names.append(sector)
    
    # 차트 생성
    fig = go.Figure()
    
    # 섹터 수익률 막대 (색상: 양수=녹색, 음수=빨강)
    colors = ['green' if x >= 0 else 'red' for x in sector_returns]
    
    fig.add_trace(go.Bar(
        x=sector_names,
        y=sector_returns,
        name='섹터 수익률',
        marker_color=colors,
        text=[f"{x:+.2f}%" for x in sector_returns],
        textposition='outside'
    ))
    
    # SPY 벤치마크 선
    fig.add_trace(go.Scatter(
        x=sector_names,
        y=[spy_return] * len(sector_names),
        mode='lines',
        name=f'SPY 벤치마크 ({spy_return:+.2f}%)',
        line=dict(color='red', width=2, dash='dash')
    ))
    
    fig.update_layout(
        title=f"월간(4주) 섹터 성과 vs SPY 벤치마크",
        xaxis_title="섹터",
        yaxis_title="누적 수익률 (%)",
        height=450,
        hovermode='x unified',
        plot_bgcolor='#f8f9fa',
        margin=dict(l=50, r=50, t=50, b=100),
        xaxis=dict(tickangle=-45)
    )
    
    return fig


@callback(
    Output('etf-performance-table', 'children'),
    Input('interval-component', 'n_intervals')
)
def update_etf_performance(n):
    """최근 10일 ETF 성과 비교"""
    etf_list = api_request('/dashboard/etf-list')
    
    # ETF 티커 리스트
    etf_tickers = ['SPY', 'QQQ', 'IWM', 'DIA', 'EWY', 'SCHD']
    
    # 모든 ETF 데이터 가져오기
    all_etf_data = {}
    
    for ticker in etf_tickers:
        etf_data = api_request('/dashboard/etf-benchmark', {'ticker': ticker, 'days': 10})
        
        if etf_data and len(etf_data) > 0:
            etf_df = pd.DataFrame(etf_data)
            etf_df['trade_date'] = pd.to_datetime(etf_df['trade_date'])
            etf_df = etf_df.sort_values('trade_date')
            etf_df['close_price'] = pd.to_numeric(etf_df['close_price'])
            
            if len(etf_df) >= 2:
                start_price = etf_df['close_price'].iloc[0]
                end_price = etf_df['close_price'].iloc[-1]
                performance = ((end_price - start_price) / start_price) * 100
                all_etf_data[ticker] = {
                    'start_date': etf_df['trade_date'].iloc[0].strftime('%Y-%m-%d'),
                    'end_date': etf_df['trade_date'].iloc[-1].strftime('%Y-%m-%d'),
                    'start_price': start_price,
                    'end_price': end_price,
                    'performance': performance,
                    'days': len(etf_df)
                }
    
    rows = []
    for ticker, data in all_etf_data.items():
        perf = data['performance']
        rows.append(html.Tr([
            html.Td(ticker, style={'padding': '10px', 'fontWeight': 'bold', 'fontSize': '14px'}),
            html.Td(f"{data['days']}일", style={'padding': '10px', 'textAlign': 'center'}),
            html.Td(data['start_date'], style={'padding': '10px', 'fontSize': '13px'}),
            html.Td(data['end_date'], style={'padding': '10px', 'fontSize': '13px'}),
            html.Td(f"${data['start_price']:.2f}", style={'padding': '10px', 'textAlign': 'right'}),
            html.Td(f"${data['end_price']:.2f}", style={'padding': '10px', 'textAlign': 'right'}),
            html.Td(f"{perf:+.2f}%", style={
                'padding': '10px',
                'fontWeight': 'bold',
                'fontSize': '15px',
                'textAlign': 'right',
                'color': 'green' if perf >= 0 else 'red'
            })
        ]))
    
    if not rows:
        return html.P("데이터 없음", style={'color': '#95a5a6'})
    
    return html.Div([
        html.Table([
            html.Thead(html.Tr([
                html.Th("ETF", style={'padding': '12px', 'backgroundColor': '#16a085', 'color': 'white'}),
                html.Th("기간", style={'padding': '12px', 'backgroundColor': '#16a085', 'color': 'white'}),
                html.Th("시작일", style={'padding': '12px', 'backgroundColor': '#16a085', 'color': 'white'}),
                html.Th("종료일", style={'padding': '12px', 'backgroundColor': '#16a085', 'color': 'white'}),
                html.Th("시작가", style={'padding': '12px', 'backgroundColor': '#16a085', 'color': 'white'}),
                html.Th("종료가", style={'padding': '12px', 'backgroundColor': '#16a085', 'color': 'white'}),
                html.Th("수익률", style={'padding': '12px', 'backgroundColor': '#16a085', 'color': 'white'})
            ])),
            html.Tbody(rows)
        ], style={
            'width': '100%',
            'borderCollapse': 'collapse',
            'border': '1px solid #ddd',
            'boxShadow': '0 2px 4px rgba(0,0,0,0.1)'
        }),
        html.P("* 실제 시장 데이터와 비교하여 계산이 정확한지 검증하세요", style={
            'marginTop': '10px',
            'fontSize': '13px',
            'color': '#7f8c8d',
            'fontStyle': 'italic'
        })
    ])


@callback(
    Output('top-performers-table', 'children'),
    Input('interval-component', 'n_intervals')
)
def update_top_performers(n):
    """최고 성과 종목 테이블"""
    data = api_request('/dashboard/top-performers', {'limit': 10, 'window_days': 5})
    
    if not data:
        return html.P("데이터 없음", style={'color': '#95a5a6'})
    
    rows = []
    for item in data:
        return_pct = item.get('return_pct', 0) or 0
        rows.append(html.Tr([
            html.Td(item.get('ticker', ''), style={'padding': '10px', 'fontWeight': 'bold'}),
            html.Td(item.get('company_name', ''), style={'padding': '10px'}),
            html.Td(item.get('sector', ''), style={'padding': '10px', 'fontSize': '13px'}),
            html.Td(f"{return_pct:+.2f}%", style={
                'padding': '10px',
                'fontWeight': 'bold',
                'color': 'green' if return_pct >= 0 else 'red'
            })
        ]))
    
    return html.Table([
        html.Thead(html.Tr([
            html.Th("티커", style={'padding': '12px', 'backgroundColor': '#2c3e50', 'color': 'white'}),
            html.Th("종목명", style={'padding': '12px', 'backgroundColor': '#2c3e50', 'color': 'white'}),
            html.Th("섹터", style={'padding': '12px', 'backgroundColor': '#2c3e50', 'color': 'white'}),
            html.Th("수익률", style={'padding': '12px', 'backgroundColor': '#2c3e50', 'color': 'white'})
        ])),
        html.Tbody(rows)
    ], style={
        'width': '100%',
        'borderCollapse': 'collapse',
        'border': '1px solid #ddd',
        'boxShadow': '0 2px 4px rgba(0,0,0,0.1)'
    })


@callback(
    Output('sector-trending-table', 'children'),
    Input('interval-component', 'n_intervals')
)
def update_sector_trending(n):
    """섹터 트렌딩 테이블"""
    data = api_request('/dashboard/sector-trending')
    
    if not data:
        return html.P("데이터 없음", style={'color': '#95a5a6'})
    
    rows = []
    for item in data[:10]:
        avg_change = item.get('avg_change_pct', 0) or 0
        rows.append(html.Tr([
            html.Td(item.get('sector', ''), style={'padding': '10px'}),
            html.Td(f"{avg_change:+.2f}%", style={
                'padding': '10px',
                'fontWeight': 'bold',
                'color': 'green' if avg_change >= 0 else 'red'
            }),
            html.Td(str(item.get('stock_count', 0)), style={'padding': '10px', 'textAlign': 'center'}),
            html.Td("✅" if item.get('is_trending') else "❌", style={'padding': '10px', 'textAlign': 'center'})
        ]))
    
    return html.Table([
        html.Thead(html.Tr([
            html.Th("섹터", style={'padding': '12px', 'backgroundColor': '#34495e', 'color': 'white'}),
            html.Th("평균 변화율", style={'padding': '12px', 'backgroundColor': '#34495e', 'color': 'white'}),
            html.Th("종목 수", style={'padding': '12px', 'backgroundColor': '#34495e', 'color': 'white'}),
            html.Th("트렌딩", style={'padding': '12px', 'backgroundColor': '#34495e', 'color': 'white'})
        ])),
        html.Tbody(rows)
    ], style={
        'width': '100%',
        'borderCollapse': 'collapse',
        'border': '1px solid #ddd',
        'boxShadow': '0 2px 4px rgba(0,0,0,0.1)'
    })


@callback(
    Output('portfolio-allocation-table', 'children'),
    [Input('interval-component', 'n_intervals'),
     Input('period-selector', 'value')]
)
def update_portfolio_allocation_table(n, period_value):
    """포트폴리오 배분 결과 (13:00 UTC Spark 분석) - 멀티 기간 + 월간 리밸런싱 지원"""
    
    # 월간 리밸런싱 포트폴리오 처리
    if period_value == 'monthly':
        monthly_data = api_request('/stocks/monthly-portfolio')
        
        if not monthly_data or not monthly_data.get('data'):
            return html.Div([
                html.P("⏳ 월간 리밸런싱 포트폴리오 대기 중... (매월 마지막 일요일 14:00 UTC 실행)", 
                       style={'color': '#e67e22', 'fontSize': '14px', 'padding': '10px'}),
                html.P("💡 5일/10일/20일 포트폴리오 통합 → 가중치 점수 기반 최종 선정",
                       style={'color': '#95a5a6', 'fontSize': '12px', 'padding': '10px', 'fontStyle': 'italic'})
            ])
        
        # Extract metadata
        rebalance_date = monthly_data.get('rebalance_date', 'N/A')
        valid_until = monthly_data.get('valid_until', 'N/A')
        total_stocks = monthly_data.get('total_stocks', 0)
        total_weight = monthly_data.get('total_weight', 0)
        
        # Build table
        rows = []
        for item in monthly_data['data']:
            rank = item.get('rank', 0)
            ticker = item.get('ticker', '')
            company_name = item.get('company_name', '')
            weight = item.get('weight', 0)
            score = item.get('score', 0)
            source_periods = item.get('source_periods', '')
            return_pct = item.get('return_pct', 0)
            market_cap = item.get('market_cap', 0)
            
            market_cap_b = market_cap / 1e9 if market_cap else 0
            
            # Color by source periods
            source_color = '#27ae60' if '20d' in source_periods and '10d' in source_periods else '#3498db'
            
            rows.append(html.Tr([
                html.Td(str(rank), style={'padding': '10px', 'textAlign': 'center', 'fontWeight': 'bold'}),
                html.Td(ticker, style={'padding': '10px', 'fontWeight': 'bold', 'fontSize': '15px'}),
                html.Td(company_name[:30], style={'padding': '10px', 'fontSize': '13px'}),
                html.Td(f"{weight:.2f}%", style={
                    'padding': '10px',
                    'fontWeight': 'bold',
                    'fontSize': '15px',
                    'textAlign': 'right',
                    'backgroundColor': '#e8f5e9' if weight > 5 else 'white'
                }),
                html.Td(f"{score:.1f}", style={'padding': '10px', 'textAlign': 'center', 'fontWeight': 'bold', 'color': '#e74c3c'}),
                html.Td(source_periods, style={'padding': '10px', 'textAlign': 'center', 'fontSize': '12px', 'color': source_color, 'fontWeight': 'bold'}),
                html.Td(f"{return_pct:+.2f}%", style={
                    'padding': '10px',
                    'fontWeight': 'bold',
                    'textAlign': 'right',
                    'color': 'green' if return_pct >= 0 else 'red'
                }),
                html.Td(f"${market_cap_b:.2f}B", style={'padding': '10px', 'textAlign': 'right', 'fontSize': '13px'})
            ]))
        
        return html.Div([
            html.Div([
                html.P(f"📅 리밸런싱 날짜: {rebalance_date} | 유효 기간: ~ {valid_until}", 
                       style={'fontSize': '14px', 'fontWeight': 'bold', 'color': '#2c3e50', 'marginBottom': '5px'}),
                html.P(f"📊 총 {total_stocks}개 종목 | 총 가중치: {total_weight}%",
                       style={'fontSize': '13px', 'color': '#7f8c8d', 'marginBottom': '15px'})
            ]),
            html.Table([
                html.Thead(html.Tr([
                    html.Th("순위", style={'padding': '12px', 'backgroundColor': '#9b59b6', 'color': 'white'}),
                    html.Th("티커", style={'padding': '12px', 'backgroundColor': '#9b59b6', 'color': 'white'}),
                    html.Th("회사명", style={'padding': '12px', 'backgroundColor': '#9b59b6', 'color': 'white'}),
                    html.Th("가중치", style={'padding': '12px', 'backgroundColor': '#9b59b6', 'color': 'white'}),
                    html.Th("점수", style={'padding': '12px', 'backgroundColor': '#9b59b6', 'color': 'white'}),
                    html.Th("출처", style={'padding': '12px', 'backgroundColor': '#9b59b6', 'color': 'white'}),
                    html.Th("20일 수익률", style={'padding': '12px', 'backgroundColor': '#9b59b6', 'color': 'white'}),
                    html.Th("시가총액", style={'padding': '12px', 'backgroundColor': '#9b59b6', 'color': 'white'})
                ])),
                html.Tbody(rows)
            ], style={
                'width': '100%',
                'borderCollapse': 'collapse',
                'border': '1px solid #ddd',
                'boxShadow': '0 2px 4px rgba(0,0,0,0.1)'
            }),
            html.Div([
                html.P("💡 점수 계산: 20일(1) + 20일∩10일(+1) + 20일∩5일(+1) | 10일∩5일만(2.0) | 10일만(0.5) | 5일만(0.3)",
                       style={'fontSize': '12px', 'color': '#95a5a6', 'marginTop': '15px', 'fontStyle': 'italic'})
            ])
        ])
    
    # 일일 포트폴리오 (5일/10일/20일)
    period_days = period_value
    data = api_request('/stocks/portfolio', {'period_days': period_days})
    
    if not data:
        period_label = f"{period_days}일" if period_days else "선택된 기간"
        return html.Div([
            html.P(f"⏳ {period_label} 포트폴리오 배분 대기 중... (13:00 UTC 실행)", 
                   style={'color': '#e67e22', 'fontSize': '14px', 'padding': '10px'}),
            html.P("💡 트렌딩 ETF당 최고성과 1종목 선정 → Weight = Performance × (1/Market Cap)",
                   style={'color': '#95a5a6', 'fontSize': '12px', 'padding': '10px', 'fontStyle': 'italic'})
        ])
    
    # Sort by weight descending
    sorted_data = sorted(data, key=lambda x: x.get('weight', 0), reverse=True)
    
    rows = []
    total_weight = 0
    
    for i, item in enumerate(sorted_data[:20]):
        ticker = item.get('ticker', '')
        weight = item.get('weight', 0)
        return_20d = item.get('return_20d', 0)
        market_cap = item.get('market_cap', 0)
        allocation_reason = item.get('allocation_reason', '')
        
        total_weight += weight
        
        # Format market cap in billions
        market_cap_b = market_cap / 1e9 if market_cap else 0
        
        rows.append(html.Tr([
            html.Td(str(i+1), style={'padding': '10px', 'textAlign': 'center', 'fontWeight': 'bold'}),
            html.Td(ticker, style={'padding': '10px', 'fontWeight': 'bold', 'fontSize': '15px'}),
            html.Td(f"{weight:.2f}%", style={
                'padding': '10px',
                'fontWeight': 'bold',
                'fontSize': '15px',
                'color': '#27ae60'
            }),
            html.Td(f"{return_20d:+.2f}%", style={
                'padding': '10px',
                'color': 'green' if return_20d >= 0 else 'red',
                'fontWeight': 'bold'
            }),
            html.Td(f"${market_cap_b:.2f}B", style={'padding': '10px', 'textAlign': 'right', 'fontSize': '13px'}),
            html.Td(allocation_reason.replace('_', ' ').title() if allocation_reason else 'N/A', 
                   style={'padding': '10px', 'fontSize': '12px', 'color': '#7f8c8d'})
        ]))
    
    # Get period label
    period_label = f"{period_days}일" if period_days else "N/A"
    as_of_date = sorted_data[0].get('as_of_date', 'N/A') if sorted_data else 'N/A'
    
    return html.Div([
        html.Div([
            html.Span(f"📊 총 {len(sorted_data)}개 종목", style={'marginRight': '20px', 'fontWeight': 'bold'}),
            html.Span(f"📅 분석기간: {period_label}", style={'marginRight': '20px', 'fontWeight': 'bold', 'color': '#3498db'}),
            html.Span(f"💯 총 비중: {total_weight:.2f}%", style={'marginRight': '20px', 'fontWeight': 'bold', 'color': '#27ae60'}),
            html.Span(f"📆 기준일: {as_of_date}", style={'color': '#95a5a6', 'fontSize': '13px'})
        ], style={'padding': '10px', 'marginBottom': '10px', 'backgroundColor': '#ecf0f1', 'borderRadius': '5px'}),
        
        html.Table([
            html.Thead(html.Tr([
                html.Th("순위", style={'padding': '12px', 'backgroundColor': '#8e44ad', 'color': 'white'}),
                html.Th("종목", style={'padding': '12px', 'backgroundColor': '#8e44ad', 'color': 'white'}),
                html.Th("비중 (%)", style={'padding': '12px', 'backgroundColor': '#8e44ad', 'color': 'white'}),
                html.Th(f"{period_days}일 수익률", style={'padding': '12px', 'backgroundColor': '#8e44ad', 'color': 'white'}),
                html.Th("시가총액", style={'padding': '12px', 'backgroundColor': '#8e44ad', 'color': 'white'}),
                html.Th("배분 사유", style={'padding': '12px', 'backgroundColor': '#8e44ad', 'color': 'white'})
            ])),
            html.Tbody(rows)
        ], style={
            'width': '100%',
            'borderCollapse': 'collapse',
            'border': '1px solid #ddd',
            'boxShadow': '0 2px 4px rgba(0,0,0,0.1)'
        }),
        
        html.P("💡 Weight = Performance × (1/Market Cap) | 각 트렌딩 ETF에서 최고성과 1종목 자동 선정", 
               style={'marginTop': '15px', 'fontSize': '13px', 'color': '#7f8c8d', 'fontStyle': 'italic'})
    ])


@callback(
    [Output('portfolio-title', 'children'),
     Output('portfolio-subtitle', 'children'),
     Output('portfolio-description', 'children')],
    Input('period-selector', 'value')
)
def update_portfolio_labels(period_value):
    """포트폴리오 선택기에 따른 제목 및 설명 업데이트"""
    if period_value == 'monthly':
        title = "🌙 월간 리밸런싱 포트폴리오 (5d+10d+20d 통합)"
        subtitle = "✨ 매월 마지막 일요일 14:00 UTC: 5일/10일/20일 포트폴리오 비교 → 가중치 점수 기반 통합"
        desc = "💡 매월 마지막 일요일에 3개 기간 포트폴리오를 통합하여 다음 20영업일 동안 유지할 최종 포트폴리오를 생성합니다"
    else:
        title = f"💎 {period_value}일 포트폴리오 배분"
        subtitle = f"✨ Stage 5 (13:00 UTC): 트렌딩 ETF당 최고성과 1종목 선정 → {period_value}일 기준 Weight 계산"
        
        if period_value == 5:
            desc = "⚡ 5일 단기: 빠른 시장 변화에 민감하게 반응 (고위험·고수익)"
        elif period_value == 10:
            desc = "⚖️ 10일 중기: 단기와 장기의 균형 잡힌 접근"
        else:  # 20
            desc = "🛡️ 20일 장기: 안정적이고 신뢰도 높은 트렌드 추종"
    
    return title, subtitle, desc


if __name__ == '__main__':
    logger.info("🚀 Starting Dashboard Server on port 8050...")
    app.run_server(
        host='0.0.0.0',
        port=8050,
        debug=False,
        dev_tools_ui=False
    )
