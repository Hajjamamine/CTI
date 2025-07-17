import socket

def get_local_ip():
    """Get the local IP address of the machine"""
    try:
        # Connect to a remote server to determine local IP
        # This doesn't actually send data, just determines routing
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("8.8.8.8", 80))
            local_ip = s.getsockname()[0]
        return local_ip
    except Exception as e:
        return f"Error getting IP: {e}"

def get_hostname_ip():
    """Alternative method using hostname"""
    try:
        hostname = socket.gethostname()
        ip_address = socket.gethostbyname(hostname)
        return ip_address
    except Exception as e:
        return f"Error getting IP: {e}"

if __name__ == "__main__":
    print("Machine IP Addresses:")
    print(f"Local IP (method 1): {get_local_ip()}")
    print(f"Local IP (method 2): {get_hostname_ip()}")
    print(f"Hostname: {socket.gethostname()}")

