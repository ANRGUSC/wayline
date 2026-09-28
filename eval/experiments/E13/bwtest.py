"""TCP throughput probe: `bwtest.py server` receives one stream on port 5999;
`bwtest.py client <ip> <MB>` sends MB megabytes and prints Mbit/s."""
import socket, sys, time
if sys.argv[1] == "server":
    s = socket.socket(); s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(("0.0.0.0", 5999)); s.listen(1); s.settimeout(60)
    c, _ = s.accept(); n = 0
    while True:
        d = c.recv(1 << 20)
        if not d:
            break
        n += len(d)
    c.sendall(b"k"); print(n)
else:
    ip, mb = sys.argv[2], int(sys.argv[3])
    s = socket.create_connection((ip, 5999)); b = b"x" * (1 << 20); t = time.time()
    for _ in range(mb):
        s.sendall(b)
    s.shutdown(socket.SHUT_WR); s.recv(1)
    print(round(mb * 8 * 1.048576 / (time.time() - t), 1))
