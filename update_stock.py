import sys
import os
import notion_client

print("\n" + "="*50)
print("👻 [유령 탐지 시작]")
print(f"📂 현재 작업 폴더: {os.getcwd()}")
print(f"📂 현재 폴더의 모든 파일/폴더 목록:\n{os.listdir()}")

print("-" * 50)
print(f"📦 로딩된 notion_client: {notion_client}")

try:
    print(f"📍 범인의 실제 위치(__file__): {notion_client.__file__}")
except:
    print("📍 범인의 실제 위치: (파일 정보 없음 - namespace 패키지일 가능성)")

try:
    print(f"🛤️ 범인의 경로(__path__): {notion_client.__path__}")
except:
    pass

print("="*50 + "\n")
