import os
import sqlite3
import socket
import sys
import threading

SERVER_NAME = "STOMP_PYTHON_SQL_SERVER"
DB_FILE = "stomp_server.db"

def recv_null_terminated(sock: socket.socket) -> str:
    data = b""
    while True:
        chunk = sock.recv(1024)
        if not chunk:
            return ""
        data += chunk
        if b"\0" in data:
            msg, _ = data.split(b"\0", 1)
            return msg.decode("utf-8", errors="replace")

def init_database():
    db_is_new = not os.path.exists(DB_FILE)
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    if db_is_new:
        print(f"[{SERVER_NAME}] Creating new database file.")
        
    cursor.execute("CREATE TABLE IF NOT EXISTS users (username TEXT PRIMARY KEY)")
    
    cursor.execute("""CREATE TABLE IF NOT EXISTS login_history 
                     (id INTEGER PRIMARY KEY AUTOINCREMENT, 
                      username TEXT, 
                      login_time TEXT, 
                      logout_time TEXT)""")
    
    cursor.execute("""CREATE TABLE IF NOT EXISTS reports 
                  (id INTEGER PRIMARY KEY AUTOINCREMENT, 
                   username TEXT, 
                   filename TEXT, 
                   game_channel TEXT, 
                   timestamp TEXT)""")
    
    conn.commit()
    conn.close()

def execute_sql_command(sql_command: str) -> str:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute(sql_command)
            conn.commit()
            return "done"
    except Exception as e:
        return f"Error: {e}"

def execute_sql_query(sql_query: str) -> str:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute(sql_query)
            rows = cursor.fetchall()
            result = "\n".join([", ".join(map(str, row)) for row in rows])
            return result if result else "empty"
    except Exception as e:
        return f"Error: {e}"

def generate_final_report():
    print("\n" + "="*45)
    print("        SERVER SUMMARY REPORT (SQL)")
    print("="*45)

    print("\n[Registered Users]:")
    users = execute_sql_query("SELECT username FROM users")
    print(users if users != "empty" else "No users registered.")

    print("\n[Login History]:")
    login_history = execute_sql_query("SELECT username, login_time, logout_time FROM login_history")
    if login_history != "empty":
        print(f"{'User':<12} | {'Login':<20} | {'Logout':<20}")
        print("-" * 55)
        for line in login_history.split('\n'):
            parts = line.split(", ")
            u = parts[0]
            in_t = parts[1] if len(parts) > 1 else "---"
            out_t = parts[2] if len(parts) > 2 else "Active"
            print(f"{u:<12} | {in_t:<20} | {out_t:<20}")
    else:
        print("No login history.")

    print("\n[Uploaded Files per User]:")
    files_report = execute_sql_query("""
        SELECT username, GROUP_CONCAT(DISTINCT filename) 
        FROM reports 
        GROUP BY username
    """)
    print(files_report if files_report != "empty" else "No files uploaded.")
    print("\n" + "="*45 + "\n")

def handle_client(client_socket: socket.socket, addr):
    print(f"[{SERVER_NAME}] Client connected from {addr}")
    try:
        while True:
            message = recv_null_terminated(client_socket)
            if message == "":
                break

            print(f"[{SERVER_NAME}] Received SQL: {message}")

            if message.strip().upper().startswith("SELECT"):
                response = execute_sql_query(message)
            else:
                response = execute_sql_command(message)
            
            client_socket.sendall((response + "\0").encode("utf-8"))
    except Exception as e:
        print(f"[{SERVER_NAME}] Error handling client {addr}: {e}")
    finally:
        client_socket.close()
        print(f"[{SERVER_NAME}] Client {addr} disconnected")

def start_server(host="127.0.0.1", port=7778):
    init_database()
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    try:
        server_socket.bind((host, port))
        server_socket.listen(5)
        print(f"[{SERVER_NAME}] Server started on {host}:{port}")
        print(f"[{SERVER_NAME}] Waiting for connections... (Press Ctrl+C to stop)")

        while True:
            client_socket, addr = server_socket.accept()
            t = threading.Thread(
                target=handle_client,
                args=(client_socket, addr),
                daemon=True
            )
            t.start()

    except KeyboardInterrupt:
        print(f"\n[{SERVER_NAME}] Server stopping...")
        generate_final_report()
    finally:
        server_socket.close()

if __name__ == "__main__":
    port = 7778
    if len(sys.argv) > 1:
        try:
            port = int(sys.argv[1].strip())
        except ValueError:
            pass
    start_server(port=port)