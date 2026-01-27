#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Kafka 유틸리티 모듈
Kafka 연결 및 메시지 생성/소비를 위한 유틸리티 함수들을 제공합니다.
"""

import json
import logging
import sys
import os
from datetime import datetime
from kafka import KafkaProducer, KafkaConsumer, errors
from kafka.admin import KafkaAdminClient, NewTopic

# 현재 디렉토리를 모듈 검색 경로에 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.config import KAFKA_CONFIG

logger = logging.getLogger(__name__)

class KafkaHelper:
    """Kafka 연결 및 메시지 관련 헬퍼 클래스"""
    
    def __init__(self):
        """Kafka 헬퍼 초기화"""
        self.config = KAFKA_CONFIG
        self.producer = None
        self.consumer = None
        self.admin_client = None
    
    def create_producer(self):
        """Kafka 프로듀서 생성"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.config['bootstrap_servers'],
                value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                key_serializer=lambda x: x.encode('utf-8') if x else None,
                client_id=self.config['client_id'],
                acks='all',
                retries=3,
                request_timeout_ms=15000
            )
            logger.info("Kafka 프로듀서가 생성되었습니다.")
            return True
        except errors.KafkaError as e:
            logger.error(f"Kafka 프로듀서 생성 실패: {e}")
            return False
    
    def create_consumer(self, topics, group_id=None):
        """Kafka 컨슈머 생성 (단일 토픽 또는 토픽 리스트 지원)"""
        try:
            if isinstance(topics, (list, tuple, set)):
                topic_list = list(topics)
            else:
                topic_list = [topics]

            self.consumer = KafkaConsumer(
                *topic_list,
                bootstrap_servers=self.config['bootstrap_servers'],
                auto_offset_reset=self.config['auto_offset_reset'],
                enable_auto_commit=self.config['enable_auto_commit'],
                group_id=group_id or self.config['consumer_group_id'],
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                max_poll_records=self.config['max_poll_records']
            )
            logger.info(f"Kafka 컨슈머가 생성되었습니다. 토픽: {topic_list}")
            return True
        except errors.KafkaError as e:
            logger.error(f"Kafka 컨슈머 생성 실패: {e}")
            return False
    
    def send_message(self, topic, value, key=None):
        """Kafka 토픽에 메시지 전송"""
        if not self.producer:
            if not self.create_producer():
                return False
        
        try:
            if 'timestamp' not in value:
                value['timestamp'] = datetime.now().isoformat()
            
            self.producer.send(topic, key=key, value=value)
            self.producer.flush()
            return True
        except Exception as e:
            logger.error(f"메시지 전송 실패: {e}")
            return False

    # Backward compatibility for callers expecting `produce`
    def produce(self, topic, value, key=None):
        return self.send_message(topic, value, key)
    
    def consume_messages(self, topics, group_id=None):
        """Kafka 토픽에서 메시지 소비"""
        if not self.consumer:
            if not self.create_consumer(topics, group_id):
                return
        
        try:
            for message in self.consumer:
                yield message
        except Exception as e:
            logger.error(f"메시지 소비 실패: {e}")
    
    def close(self):
        """연결 종료"""
        if self.producer:
            self.producer.close()
        if self.consumer:
            self.consumer.close()
        if self.admin_client:
            self.admin_client.close()
