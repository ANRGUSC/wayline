# Contributing to Wayline

Thanks for your interest. Wayline is a research artifact plus a working
Kubernetes-native runtime for one-shot DAGs.

## Layout & docs
- Architecture and data-plane internals: [`docs/architecture.md`](docs/architecture.md)
- Getting started / local dev: [`docs/getting-started.md`](docs/getting-started.md), [`docs/local-dev.md`](docs/local-dev.md)
- Writing tasks (Python SDK): [`docs/sdk-quickstart.md`](docs/sdk-quickstart.md)
- Reproducing the evaluation: [`eval/README.md`](eval/README.md)

## Building & testing
```
make build      # Go binaries (odag-controller, data-agent, ui-server, wayline CLI)
make ui-build   # React UI
make test       # Go unit tests
```
The controller is the only place scheduling logic lives
(`cmd/odag-controller/`). The data plane is `cmd/data-agent/`. CRDs are raw YAML
under `api/v1/` (the controller works with `unstructured` objects, not generated
structs).

## Pull requests
- Keep changes focused; match the surrounding code's style.
- If you touch the scheduler or data-plane wire protocol, note the impact on the
  invariants in `eval/data-agent-tests/`.
- Run `make build && make test` before submitting.

## License
By contributing you agree your contributions are licensed under the repository's
[MIT License](LICENSE).
