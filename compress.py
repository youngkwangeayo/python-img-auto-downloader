#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
이미지 폴더 압축 프로그램
downloaded_images 폴더를 안전하게 압축하여 전송 가능한 파일로 생성합니다.
"""

import os
import zipfile
import tarfile
from datetime import datetime
from pathlib import Path


class ImageCompressor:
    def __init__(self, source_dir='./downloaded_images', output_dir='./compressed'):
        """
        압축기 초기화

        Args:
            source_dir: 압축할 소스 디렉토리
            output_dir: 압축 파일을 저장할 디렉토리
        """
        self.source_dir = Path(source_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # 로그 디렉토리 및 날짜별 로그 파일 설정
        log_dir = Path('./log/compress')
        log_dir.mkdir(parents=True, exist_ok=True)
        today = datetime.now().strftime('%Y%m%d')
        self.log_file = log_dir / f"{today}_compress.log"

    def log(self, message, level='INFO'):
        """로그 메시지를 파일과 콘솔에 출력"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        log_message = f"[{timestamp}] [{level}] {message}"
        print(log_message)

        with open(self.log_file, 'a', encoding='utf-8') as f:
            f.write(log_message + '\n')

    def get_folder_size(self, folder_path):
        """폴더의 전체 크기 계산 (바이트)"""
        total_size = 0
        try:
            for dirpath, dirnames, filenames in os.walk(folder_path):
                for filename in filenames:
                    filepath = os.path.join(dirpath, filename)
                    if os.path.exists(filepath):
                        total_size += os.path.getsize(filepath)
        except Exception as e:
            self.log(f"폴더 크기 계산 중 오류: {e}", 'ERROR')
        return total_size

    def format_size(self, size_bytes):
        """바이트를 읽기 쉬운 형식으로 변환"""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size_bytes < 1024.0:
                return f"{size_bytes:.2f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.2f} TB"

    def count_files(self, folder_path):
        """폴더 내 파일 개수 계산"""
        file_count = 0
        try:
            for dirpath, dirnames, filenames in os.walk(folder_path):
                file_count += len(filenames)
        except Exception as e:
            self.log(f"파일 개수 계산 중 오류: {e}", 'ERROR')
        return file_count

    def create_zip(self, output_filename=None, compression_level=6):
        """
        ZIP 파일 생성

        Args:
            output_filename: 출력 파일명 (None이면 자동 생성)
            compression_level: 압축 레벨 (0-9, 기본값 6)

        Returns:
            생성된 압축 파일 경로
        """
        if not self.source_dir.exists():
            self.log(f"오류: 소스 디렉토리를 찾을 수 없습니다: {self.source_dir}", 'ERROR')
            return None

        if not output_filename:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_filename = f"images_{timestamp}.zip"

        output_path = self.output_dir / output_filename

        self.log("=" * 80)
        self.log("이미지 폴더 압축 시작")
        self.log("=" * 80)

        file_count = self.count_files(self.source_dir)
        folder_size = self.get_folder_size(self.source_dir)

        self.log(f"소스 디렉토리: {self.source_dir}")
        self.log(f"파일 개수: {file_count}개")
        self.log(f"전체 크기: {self.format_size(folder_size)}")
        self.log(f"압축 레벨: {compression_level}/9")
        self.log(f"출력 파일: {output_path}")
        self.log("-" * 80)

        try:
            processed_files = 0

            with zipfile.ZipFile(
                output_path,
                'w',
                zipfile.ZIP_DEFLATED,
                compresslevel=compression_level
            ) as zipf:
                for root, dirs, files in os.walk(self.source_dir):
                    for file in files:
                        file_path = os.path.join(root, file)
                        arcname = os.path.relpath(file_path, self.source_dir)

                        zipf.write(file_path, arcname)
                        processed_files += 1

                        if processed_files % 50 == 0:
                            progress = (processed_files / file_count) * 100
                            self.log(f"진행 중: {processed_files}/{file_count} ({progress:.1f}%)")

            compressed_size = os.path.getsize(output_path)
            compression_ratio = (1 - compressed_size / folder_size) * 100 if folder_size > 0 else 0

            self.log("-" * 80)
            self.log("압축 완료!")
            self.log(f"압축 파일: {output_path}")
            self.log(f"원본 크기: {self.format_size(folder_size)}")
            self.log(f"압축 크기: {self.format_size(compressed_size)}")
            self.log(f"압축률: {compression_ratio:.1f}%")
            self.log("=" * 80)

            return output_path

        except Exception as e:
            self.log(f"압축 중 오류 발생: {e}", 'ERROR')
            if output_path.exists():
                output_path.unlink()
            return None

    def create_tar_gz(self, output_filename=None):
        """
        TAR.GZ 파일 생성 (리눅스/맥 환경에서 선호)

        Args:
            output_filename: 출력 파일명 (None이면 자동 생성)

        Returns:
            생성된 압축 파일 경로
        """
        if not self.source_dir.exists():
            self.log(f"오류: 소스 디렉토리를 찾을 수 없습니다: {self.source_dir}", 'ERROR')
            return None

        if not output_filename:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_filename = f"images_{timestamp}.tar.gz"

        output_path = self.output_dir / output_filename

        self.log("=" * 80)
        self.log("이미지 폴더 압축 시작 (TAR.GZ)")
        self.log("=" * 80)

        file_count = self.count_files(self.source_dir)
        folder_size = self.get_folder_size(self.source_dir)

        self.log(f"소스 디렉토리: {self.source_dir}")
        self.log(f"파일 개수: {file_count}개")
        self.log(f"전체 크기: {self.format_size(folder_size)}")
        self.log(f"출력 파일: {output_path}")
        self.log("-" * 80)

        try:
            processed_files = 0

            with tarfile.open(output_path, 'w:gz') as tar:
                for root, dirs, files in os.walk(self.source_dir):
                    for file in files:
                        file_path = os.path.join(root, file)
                        arcname = os.path.relpath(file_path, self.source_dir)

                        tar.add(file_path, arcname=arcname)
                        processed_files += 1

                        if processed_files % 50 == 0:
                            progress = (processed_files / file_count) * 100
                            self.log(f"진행 중: {processed_files}/{file_count} ({progress:.1f}%)")

            compressed_size = os.path.getsize(output_path)
            compression_ratio = (1 - compressed_size / folder_size) * 100 if folder_size > 0 else 0

            self.log("-" * 80)
            self.log("압축 완료!")
            self.log(f"압축 파일: {output_path}")
            self.log(f"원본 크기: {self.format_size(folder_size)}")
            self.log(f"압축 크기: {self.format_size(compressed_size)}")
            self.log(f"압축률: {compression_ratio:.1f}%")
            self.log("=" * 80)

            return output_path

        except Exception as e:
            self.log(f"압축 중 오류 발생: {e}", 'ERROR')
            if output_path.exists():
                output_path.unlink()
            return None

    def compress(self, format='zip', output_filename=None, compression_level=6):
        """
        통합 압축 메서드

        Args:
            format: 압축 형식 ('zip' 또는 'tar.gz')
            output_filename: 출력 파일명
            compression_level: 압축 레벨 (zip만 해당)

        Returns:
            생성된 압축 파일 경로
        """
        if format.lower() == 'zip':
            return self.create_zip(output_filename, compression_level)
        elif format.lower() in ['tar.gz', 'targz', 'tgz']:
            return self.create_tar_gz(output_filename)
        else:
            self.log(f"지원하지 않는 압축 형식: {format}", 'ERROR')
            self.log("지원 형식: zip, tar.gz")
            return None


def main():
    """프로그램 진입점"""
    import argparse

    parser = argparse.ArgumentParser(description='이미지 폴더 압축 프로그램')
    parser.add_argument(
        '-f', '--format',
        choices=['zip', 'tar.gz'],
        default='zip',
        help='압축 형식 (기본값: zip)'
    )
    parser.add_argument(
        '-o', '--output',
        help='출력 파일명 (기본값: 자동 생성)'
    )
    parser.add_argument(
        '-l', '--level',
        type=int,
        choices=range(0, 10),
        default=6,
        help='압축 레벨 0-9 (기본값: 6, zip만 해당)'
    )
    parser.add_argument(
        '-s', '--source',
        default='./downloaded_images',
        help='압축할 소스 디렉토리 (기본값: ./downloaded_images)'
    )
    parser.add_argument(
        '-d', '--dest',
        default='./compressed',
        help='압축 파일 저장 디렉토리 (기본값: ./compressed)'
    )

    args = parser.parse_args()

    compressor = ImageCompressor(source_dir=args.source, output_dir=args.dest)
    result = compressor.compress(
        format=args.format,
        output_filename=args.output,
        compression_level=args.level
    )

    if result:
        compressor.log(f"\n✓ 압축 파일 생성 완료: {result}")
        return 0
    else:
        compressor.log(f"\n✗ 압축 실패", 'ERROR')
        return 1


if __name__ == '__main__':
    import sys
    sys.exit(main())
