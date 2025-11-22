import os
import time
import webbrowser
import http.server
import socketserver
import threading
from functools import partial


# --- CẤU HÌNH ---
PORT = 8000
HTML_FILE = 'viewer_lazy.html'

def start_server():
    """Hàm chạy Local Server trong luồng riêng"""
    # Đảm bảo server chạy đúng thư mục chứa file script
    directory = os.path.dirname(os.path.abspath(__file__))
    
    # Cấu hình Handler để phục vụ file từ thư mục hiện tại
    handler = partial(http.server.SimpleHTTPRequestHandler, directory=directory)
    
    try:
        with socketserver.TCPServer(("", PORT), handler) as httpd:
            print(f"\n🚀 Server đang chạy tại: http://localhost:{PORT}")
            print("❌ Nhấn Ctrl+C trong cửa sổ này để dừng chương trình.")
            httpd.serve_forever()
    except OSError as e:
        print(f"\n⚠️ Cổng {PORT} đang bận. Có thể server đã chạy rồi.")

def main():

    # 2. KHỞI ĐỘNG SERVER (Background)
    print(f"\n--- BƯỚC 2: KHỞI ĐỘNG WEB APP ---")
    server_thread = threading.Thread(target=start_server)
    server_thread.daemon = True # Tự tắt khi chương trình chính tắt
    server_thread.start()

    # Đợi xíu cho server lên sóng
    time.sleep(1.5)

    # 3. TỰ ĐỘNG MỞ TRÌNH DUYỆT
    url = f"http://localhost:{PORT}/{HTML_FILE}"
    print(f"Dang mở trình duyệt: {url}")
    webbrowser.open(url)

    # Giữ chương trình chạy để server không bị tắt
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n🛑 Đã dừng chương trình.")

if __name__ == "__main__":
    main()