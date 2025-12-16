#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
이미지 자동 저장 프로그램
FRANCHISEE 테이블에서 로고 이미지를 다운로드하여 로컬에 저장합니다.
"""

import json
import os
import random
import sys
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse
import re

import pymysql
import requests
from sshtunnel import SSHTunnelForwarder


class ImageDownloader:
    def __init__(self, config_path='config.json'):
        """설정 파일을 로드하고 초기화"""
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = json.load(f)

        self.db_config = self.config['database']
        self.ssh_config = self.config.get('ssh_tunnel', {})
        self.s3_base_url = self.config['s3_base_url']
        self.batch_size = self.config['batch_size']
        self.output_dir = Path(self.config['output_directory'])
        self.log_file = self.config['log_file']

        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.stats = {
            'total': 0,
            'success': 0,
            'failed': 0,
            'failed_ids': []
        }

        self.ssh_tunnel = None
        self.connection = None

    def log(self, message, level='INFO'):
        """로그 메시지를 파일과 콘솔에 출력"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        log_message = f"[{timestamp}] [{level}] {message}"
        print(log_message)

        with open(self.log_file, 'a', encoding='utf-8') as f:
            f.write(log_message + '\n')

    def connect_db(self):
        """데이터베이스 연결 (SSH 터널 옵션 포함)"""
        try:
            if self.ssh_config.get('enabled', False):
                self.log("SSH 터널을 통한 데이터베이스 연결 시도...")
                self.ssh_tunnel = SSHTunnelForwarder(
                    (self.ssh_config['ssh_host'], 22),
                    ssh_username=self.ssh_config['ssh_user'],
                    ssh_pkey=self.ssh_config['ssh_key_path'],
                    remote_bind_address=(
                        self.ssh_config['remote_bind_address'],
                        self.ssh_config['remote_bind_port']
                    )
                )
                self.ssh_tunnel.start()

                self.connection = pymysql.connect(
                    host='127.0.0.1',
                    port=self.ssh_tunnel.local_bind_port,
                    user=self.db_config['user'],
                    password=self.db_config['password'],
                    database=self.db_config['database'],
                    charset='utf8mb4',
                    cursorclass=pymysql.cursors.DictCursor
                )
                self.log("SSH 터널을 통한 데이터베이스 연결 성공")
            else:
                self.log("직접 데이터베이스 연결 시도...")
                self.connection = pymysql.connect(
                    host=self.db_config['host'],
                    port=self.db_config['port'],
                    user=self.db_config['user'],
                    password=self.db_config['password'],
                    database=self.db_config['database'],
                    charset='utf8mb4',
                    cursorclass=pymysql.cursors.DictCursor
                )
                self.log("데이터베이스 직접 연결 성공")

            return True
        except Exception as e:
            self.log(f"데이터베이스 연결 실패: {str(e)}", 'ERROR')
            if not self.ssh_config.get('enabled', False):
                self.log("SSH 터널을 사용하려면 config.json에서 ssh_tunnel.enabled를 true로 설정하세요.", 'INFO')
            return False

    def close_db(self):
        """데이터베이스 연결 종료"""
        if self.connection:
            self.connection.close()
            self.log("데이터베이스 연결 종료")

        if self.ssh_tunnel:
            self.ssh_tunnel.stop()
            self.log("SSH 터널 종료")

    def get_total_count(self):
        """전체 데이터 건수 조회"""
        try:
            with self.connection.cursor() as cursor:
                query = """
                    SELECT COUNT(*) as total_count
                    FROM FRANCHISEE as f
                    WHERE f.`STATUS` = "A"
                        AND f.logoImageUrl IS NOT NULL
                        AND TRIM(f.logoImageUrl) <> ''
                """
                cursor.execute(query)
                result = cursor.fetchone()
                return result['total_count']
        except Exception as e:
            self.log(f"전체 건수 조회 실패: {str(e)}", 'ERROR')
            return 0

    def fetch_franchisee_data(self, offset, limit):
        """프랜차이즈 데이터 배치 조회"""
        try:
            with self.connection.cursor() as cursor:
                query = """
                    SELECT f.FR_ID, f.FR_NM, f.logoImageUrl as img
                    FROM FRANCHISEE as f
                    WHERE f.`STATUS` = "A"
                        AND f.logoImageUrl IS NOT NULL
                        AND TRIM(f.logoImageUrl) <> ''
                    LIMIT %s, %s
                """
                cursor.execute(query, (offset, limit))
                return cursor.fetchall()
        except Exception as e:
            self.log(f"데이터 조회 실패 (offset: {offset}): {str(e)}", 'ERROR')
            return []

    def build_image_url(self, img_path):
        """완전한 이미지 URL 생성"""
        if not img_path:
            return None

        img_path = img_path.strip()
        if img_path.startswith('/'):
            img_path = img_path[1:]

        return self.s3_base_url + img_path

    def download_image(self, url, timeout=30):
        """이미지 다운로드"""
        try:
            response = requests.get(url, timeout=timeout, stream=True)
            response.raise_for_status()
            return response.content
        except requests.exceptions.RequestException as e:
            self.log(f"이미지 다운로드 실패 ({url}): {str(e)}", 'ERROR')
            return None

    def extract_extension(self, img_path):
        """이미지 경로에서 확장자 추출"""
        if not img_path:
            return 'jpg'

        parsed = urlparse(img_path)
        path = parsed.path

        ext = os.path.splitext(path)[1]
        if ext:
            return ext[1:].lower()

        return 'jpg'

    def generate_random_digits(self):
        """랜덤 4자리 숫자 생성"""
        return str(random.randint(1000, 9999))

    def sanitize_filename(self, name):
        """파일명에 사용할 수 없는 문자 제거"""
        if not name:
            return 'unknown'

        name = re.sub(r'[\\/*?:"<>|]', '_', name)
        name = name.strip()

        if not name:
            return 'unknown'

        return name

    def generate_filename(self, fr_id, fr_nm, extension):
        """파일명 생성 (중복 시 랜덤 숫자 변경)"""
        fr_id = self.sanitize_filename(str(fr_id))
        fr_nm = self.sanitize_filename(str(fr_nm))

        max_attempts = 100
        for _ in range(max_attempts):
            random_digits = self.generate_random_digits()
            filename = f"{fr_id}_{fr_nm}_{random_digits}.{extension}"
            filepath = self.output_dir / filename

            if not filepath.exists():
                return filepath

        timestamp = int(time.time())
        filename = f"{fr_id}_{fr_nm}_{timestamp}.{extension}"
        return self.output_dir / filename

    def save_image(self, image_data, filepath):
        """이미지 파일로 저장"""
        try:
            with open(filepath, 'wb') as f:
                f.write(image_data)
            return True
        except Exception as e:
            self.log(f"파일 저장 실패 ({filepath}): {str(e)}", 'ERROR')
            return False

    def process_single_image(self, record):
        """단일 이미지 처리"""
        fr_id = record['FR_ID']
        fr_nm = record['FR_NM']
        img_path = record['img']

        image_url = self.build_image_url(img_path)
        if not image_url:
            self.log(f"[FR_ID: {fr_id}] 이미지 URL이 유효하지 않음", 'WARNING')
            return False

        image_data = self.download_image(image_url)
        if not image_data:
            return False

        extension = self.extract_extension(img_path)
        filepath = self.generate_filename(fr_id, fr_nm, extension)

        if self.save_image(image_data, filepath):
            self.log(f"[FR_ID: {fr_id}] 이미지 저장 성공: {filepath.name}")
            return True
        else:
            return False

    def process_batch(self, offset, limit, batch_num, total_batches):
        """단일 배치 처리"""
        self.log(f"배치 {batch_num}/{total_batches} 처리 시작 (offset: {offset}, limit: {limit})")

        records = self.fetch_franchisee_data(offset, limit)
        if not records:
            self.log(f"배치 {batch_num}: 조회된 데이터 없음", 'WARNING')
            return

        batch_success = 0
        batch_failed = 0

        for i, record in enumerate(records, 1):
            fr_id = record['FR_ID']

            try:
                if self.process_single_image(record):
                    batch_success += 1
                    self.stats['success'] += 1
                else:
                    batch_failed += 1
                    self.stats['failed'] += 1
                    self.stats['failed_ids'].append(fr_id)

                if i % 50 == 0:
                    self.log(f"배치 {batch_num}: {i}/{len(records)} 처리 완료")

            except Exception as e:
                self.log(f"[FR_ID: {fr_id}] 처리 중 오류: {str(e)}", 'ERROR')
                batch_failed += 1
                self.stats['failed'] += 1
                self.stats['failed_ids'].append(fr_id)

        self.log(f"배치 {batch_num} 완료 - 성공: {batch_success}, 실패: {batch_failed}")

    def run(self):
        """메인 프로세스 실행"""
        start_time = time.time()
        self.log("=" * 80)
        self.log("이미지 다운로드 프로그램 시작")
        self.log("=" * 80)

        if not self.connect_db():
            self.log("데이터베이스 연결 실패로 프로그램 종료", 'ERROR')
            return

        try:
            total_count = self.get_total_count()
            self.stats['total'] = total_count
            self.log(f"전체 대상 건수: {total_count}")

            if total_count == 0:
                self.log("처리할 데이터가 없습니다.", 'WARNING')
                return

            total_batches = (total_count + self.batch_size - 1) // self.batch_size
            self.log(f"총 배치 수: {total_batches} (배치 크기: {self.batch_size})")

            for batch_num in range(1, total_batches + 1):
                offset = (batch_num - 1) * self.batch_size
                self.process_batch(offset, self.batch_size, batch_num, total_batches)

            elapsed_time = time.time() - start_time
            self.log("=" * 80)
            self.log("처리 완료")
            self.log(f"전체 대상: {self.stats['total']}")
            self.log(f"성공: {self.stats['success']}")
            self.log(f"실패: {self.stats['failed']}")
            self.log(f"실행 시간: {elapsed_time:.2f}초")

            if self.stats['failed_ids']:
                self.log(f"실패한 FR_ID 목록 ({len(self.stats['failed_ids'])}개):")
                self.log(", ".join(map(str, self.stats['failed_ids'])))

            self.log("=" * 80)

        except KeyboardInterrupt:
            self.log("\n사용자에 의해 중단되었습니다.", 'WARNING')

        except Exception as e:
            self.log(f"예상치 못한 오류 발생: {str(e)}", 'ERROR')
            import traceback
            self.log(traceback.format_exc(), 'ERROR')

        finally:
            self.close_db()


def main():
    """프로그램 진입점"""
    if not os.path.exists('config.json'):
        print("ERROR: config.json 파일을 찾을 수 없습니다.")
        print("config.json 파일을 생성하고 데이터베이스 정보를 입력하세요.")
        sys.exit(1)

    downloader = ImageDownloader()
    downloader.run()


if __name__ == '__main__':
    main()
