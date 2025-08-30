#!/usr/bin/env python3
import http.server
import socketserver
import os
import sys
import socket
import signal
import subprocess

def find_process_on_port(port):
    """Find process using the given port"""
    try:
        result = subprocess.run(['lsof', '-i', f':{port}'], 
                              capture_output=True, text=True)
        if result.stdout:
            lines = result.stdout.strip().split('\n')[1:]  # Skip header
            for line in lines:
                parts = line.split()
                if len(parts) >= 2:
                    return parts[1]  # PID
    except:
        pass
    return None

def kill_process(pid):
    """Kill a process by PID"""
    try:
        os.kill(int(pid), signal.SIGTERM)
        return True
    except:
        return False

def is_port_in_use(port):
    """Check if a port is in use"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('localhost', port)) == 0

def serve(port=8000):
    """Start HTTP server"""
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    
    Handler = http.server.SimpleHTTPRequestHandler
    
    while True:
        if is_port_in_use(port):
            print(f"\nPort {port} is already in use!")
            pid = find_process_on_port(port)
            
            if pid:
                print(f"Process {pid} is using port {port}")
                response = input(f"Do you want to kill it and try again? (y/n): ").lower()
                
                if response == 'y':
                    if kill_process(pid):
                        print(f"Killed process {pid}")
                        import time
                        time.sleep(1)  # Give it a moment
                        continue
                    else:
                        print(f"Failed to kill process {pid}")
            
            # Try alternative ports
            print("\nAlternative options:")
            print("1. Try a different port")
            print("2. Exit")
            
            choice = input("Enter choice (1-2): ")
            
            if choice == '1':
                try:
                    port = int(input("Enter port number: "))
                except ValueError:
                    print("Invalid port number")
                    sys.exit(1)
            else:
                sys.exit(0)
        else:
            break
    
    with socketserver.TCPServer(("", port), Handler) as httpd:
        print(f"\nServing at http://localhost:{port}")
        print("Press Ctrl+C to stop")
        
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\nShutting down server...")
            httpd.shutdown()

if __name__ == "__main__":
    port = 8000
    if len(sys.argv) > 1:
        try:
            port = int(sys.argv[1])
        except ValueError:
            print("Usage: python serve.py [port]")
            sys.exit(1)
    
    serve(port)