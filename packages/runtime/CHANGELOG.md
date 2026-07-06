# Changelog

## 1.0.0 (2026-07-06)


### Features

* Added a process exit to make sure the system runs in optimal state when starting, and updated the readme constraints to make it clearer ([436aefd](https://github.com/shide1989/pipelines-ts/commit/436aefd7c879f85999987c5eeec429416f5834cd))
* advisory-lock worker liveness — reserve() sessions, lock-gated execution ([40a103b](https://github.com/shide1989/pipelines-ts/commit/40a103b9c25f0088361cf75a497316e92d026a88))
* harden runtime — replayRun creates new run, durable steps throw outside workflow context ([8eb33aa](https://github.com/shide1989/pipelines-ts/commit/8eb33aabeefb5e96e7b92cbbf0c04477db2f2369))
* implement V0.6 durable engine (event-driven worker, DB-agnostic) ([119180c](https://github.com/shide1989/pipelines-ts/commit/119180cc77fea14fb40adad7dac364bc76c26e86))
* pool-size sweep in bench; document the reserve() pool-sizing floor ([e4c23cb](https://github.com/shide1989/pipelines-ts/commit/e4c23cbf09aaa1e2c2c267b9c7633468e5df08f9))
* **publish:** publish runtime as pipelines-ts via tsc-only ESM build ([e1411d5](https://github.com/shide1989/pipelines-ts/commit/e1411d5639716e45ec39047d9c46f93d4b4901cc))
* throughput/latency benchmark (bun run bench in packages/runtime) ([4bcd75b](https://github.com/shide1989/pipelines-ts/commit/4bcd75bf1d7a875fdd018e841110644ccc8b2004))
* updated the spec, and renamed durable method to checkpoint for sake of clarity ([ab4a4c1](https://github.com/shide1989/pipelines-ts/commit/ab4a4c19a5690ef3d417a2c6d69cccd9ea7d2176))


### Bug Fixes

* make crash recovery real — reclaim orphaned runs, self-healing timers ([fa7a8d8](https://github.com/shide1989/pipelines-ts/commit/fa7a8d8d71469dde2829a4284449c02367e700d8))
* **publish:** include schema.sql in the published tarball ([e959cc3](https://github.com/shide1989/pipelines-ts/commit/e959cc3d4f1e6c63ea3a766eff295ca5fd4f9dca))
* **publish:** include schema.sql in the published tarball ([c907ec1](https://github.com/shide1989/pipelines-ts/commit/c907ec1f6c07fdcdab4825d2447cd3e39bd1e27d))


### Performance Improvements

* fold log writes into their state writes (data-modifying CTEs) ([6334896](https://github.com/shide1989/pipelines-ts/commit/63348963283f26874a51b5136cf36f91c3976b78))
