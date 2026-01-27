#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
로깅 유틸리티 모듈
일관된 로깅 설정을 제공합니다.
"""

import os
import logging
from logging.handlers import RotatingFileHandler
import sys

# 현재 디렉토리를 모듈 검색 경로에 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.config import LOG_LEVEL, LOG_FORMAT, LOG_DIR, MAX_LOG_SIZE, LOG_BACKUP_COUNT

def setup_logger(name, log_file=None):
    """
    로거 설정을 위한 도우미 함수
    
    Args:
        name (str): 로거 이름
        log_file (str, optional): 로그 파일 이름. 기본값은 None으로, 콘솔에만 출력합니다.
    
    Returns:
        logging.Logger: 설정된 로거 객체
    """
    # 로거 객체 생성
    logger = logging.getLogger(name)
    
    # 로그 레벨 설정
    log_level = getattr(logging, LOG_LEVEL)
    logger.setLevel(log_level)
    
    # 모든 핸들러 제거
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # 포맷터 설정
    formatter = logging.Formatter(LOG_FORMAT)
    
    # 콘솔 핸들러 설정
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # 파일 핸들러 설정 (로그 파일이 제공된 경우)
    if log_file:
        # 로그 디렉토리가 없는 경우 생성
        if not os.path.exists(LOG_DIR):
            os.makedirs(LOG_DIR)
        
        # 전체 로그 파일 경로
        log_path = os.path.join(LOG_DIR, log_file)
        
        # 회전 파일 핸들러 설정
        file_handler = RotatingFileHandler(
            log_path,
            maxBytes=MAX_LOG_SIZE,
            backupCount=LOG_BACKUP_COUNT
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger
