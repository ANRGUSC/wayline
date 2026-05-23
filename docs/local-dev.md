# Local Development Guide

## How the UI is served

The UI runs as a **local binary**, not from the k3s cluster:

```bash
./bin/ui-server -kubeconfig /home/anrg/.kube/config -db /tmp/wl-history.db
```

- Listens on `:8080`
- Serves the compiled React frontend from `./ui/dist/`
- Proxies Kubernetes API calls using `~/.kube/config`
- Access at **http://192.168.1.163:8080**

> **Do NOT build/push Docker images or `kubectl rollout restart` to see UI changes.**
> The k3s pod is irrelevant during development. Only the local binary matters.

---

## Rebuilding after changes

Any time you change Go or frontend code, run:

```bash
# 1. Rebuild the React frontend
npm --prefix ui run build

# 2. Rebuild the Go binary
go build -o bin/ui-server ./cmd/ui-server

# 3. Kill the old process and restart
pkill -f 'bin/ui-server'
./bin/ui-server -kubeconfig /home/anrg/.kube/config -db /tmp/wl-history.db &
```

---

## The port 8080 trap (March 2026 incident)

### What happened

Spent several hours where every UI change appeared to have no effect, even after confirmed-good Docker builds and k3s rollouts.

### Root cause

An old **phase-1 `data-agent` DaemonSet** in the `kube-system` namespace (59 days old, never cleaned up) had `hostPort: 8080` on every node:

```yaml
ports:
  - containerPort: 8080
    hostPort: 8080   # ← squatting on the port used by ui-server
```

This old data-agent and the local `./bin/ui-server` were coexisting on port 8080 because one used IPv6 and the other IPv4. The browser was always hitting the **local binary**, which was never updated. All Docker builds went nowhere.

When the local binary was killed (thinking it was the culprit), the data-agent still held the port, so the new binary couldn't start either.

### Fix

```bash
kubectl delete daemonset data-agent -n kube-system
```

### How to check if something is squatting on 8080

```bash
sudo lsof -i :8080 -n -P
```

Expected output (healthy state) — only your local ui-server:

```
COMMAND     PID  USER  ...  NAME
ui-server  XXXX  anrg  ...  *:8080 (LISTEN)
```

If you see anything else (data-agent, old binary, etc.), kill it first.

---

## Checking what's running

```bash
# Is the local ui-server up?
pgrep -a ui-server

# Is port 8080 free / who holds it?
sudo lsof -i :8080 -n -P

# Quick smoke test
curl -s http://localhost:8080/api/odags | python3 -m json.tool | head -10
```

---

## Browser cache trap

Even after rebuilding, changes may be invisible because the browser cached the old JS bundle.

**Root cause:** Vite uses content-based hashes in filenames (e.g. `index-CFar1slw.js`). If the hash doesn't change between two builds, the browser serves the old cached file. The ui-server now sets `Cache-Control: no-cache` on all assets to prevent this, but older binaries didn't.

**Fix:** Rebuild and restart the Go binary (not just the frontend):

```bash
go build -o bin/ui-server ./cmd/ui-server
pkill -f 'bin/ui-server'
./bin/ui-server -kubeconfig /home/anrg/.kube/config -db /tmp/wl-history.db &
```

If still stuck, hard refresh: `Ctrl+Shift+R` (Chrome/Firefox) or `Cmd+Shift+R` (Mac).

---

## Docker / k3s builds (for cluster deployment only)

These are only needed when deploying to the cluster for a real run — **not for UI development**:

```bash
# UI + server
docker build -f cmd/ui-server/Dockerfile -t 192.168.1.163:5000/ui-server:latest .
docker push 192.168.1.163:5000/ui-server:latest
kubectl rollout restart deployment/ui-server -n wl-system

# Controllers
docker build -f cmd/odag-controller/Dockerfile -t 192.168.1.163:5000/odag-controller:latest .
docker push 192.168.1.163:5000/odag-controller:latest
kubectl rollout restart deployment/odag-controller -n wl-system
```
