#!/usr/bin/env python3
"""
🌐 Servidor local para el Dashboard de Trading
Ejecuta este script y abre http://localhost:8080 en tu navegador
"""
import http.server
import socketserver
import webbrowser
import os

PORT = 8080

class Handler(http.server.SimpleHTTPRequestHandler):
    def log_message(self, format, *args):
        pass  # Silenciar logs del servidor

os.chdir(os.path.dirname(os.path.abspath(__file__)))

print("=" * 50)
print("  🚀 Marco Trading Dashboard")
print(f"  Abriendo en http://localhost:{PORT}")
print("  Ctrl+C para detener")
print("=" * 50)

webbrowser.open(f"http://localhost:{PORT}/dashboard.html")

with socketserver.TCPServer(("", PORT), Handler) as httpd:
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n  ✅ Servidor detenido.")
