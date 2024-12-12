import socket

def check_spark_port(start_port=4040, max_port=4050, host='127.0.0.1'):
    for port in range(start_port, max_port + 1):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.connect((host, port))
                print(f"Spark is running on port: {port}")
                return port
            except (socket.timeout, ConnectionRefusedError):
                continue
    print("Spark is not running in the specified port range.")
    return None

# Check for Spark on localhost in the default range
check_spark_port()