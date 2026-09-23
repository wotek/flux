# Changelog

## [1.1.0](https://github.com/wotek/flux/compare/v1.0.1...v1.1.0) (2026-09-23)


### Features

* **store:** add in-memory snapshot store implementation ([5ec6baa](https://github.com/wotek/flux/commit/5ec6baa3be5b7279fdc4072cdc2a5f4eaff774a5))

## [1.0.1](https://github.com/wotek/flux/compare/v1.0.0...v1.0.1) (2026-09-23)


### Bug Fixes

* trigger patch release ([e1b7427](https://github.com/wotek/flux/commit/e1b74276b61ebcb44b741cdf578430343d295c70))

## [2.2.0](https://github.com/wotek/flux/compare/v2.1.0...v2.2.0) (2026-09-23)


### Features

* **codec:** implement Protocol Buffers serializer and pointer type registry ([75ca751](https://github.com/wotek/flux/commit/75ca75189fa9b8f8012d23578a8728ea1b64439d))
* **repo:** use ULID instead of UUID for auto-generated Event Resource IDs ([7df9ba6](https://github.com/wotek/flux/commit/7df9ba6bd3b5890a888bef34a455f62515dbfe64))
* **store:** add mysql event and snapshot store implementations ([8d44542](https://github.com/wotek/flux/commit/8d44542516814c60c2800d763a018f2a9e9009f2))
* **store:** implement Redis EventStore and SnapshotStore with optimistic concurrency and global tailing ([4cf4141](https://github.com/wotek/flux/commit/4cf4141e2ad9ac37a5e95479aa54b52c4166bedc))


### Bug Fixes

* **event:** use sentinel error for RegisterType panic ([f0a51a9](https://github.com/wotek/flux/commit/f0a51a907467d262b10447521bb6eb11cf64799e))

## [2.1.0](https://github.com/wotek/flux/compare/v2.0.1...v2.1.0) (2026-09-23)


### Features

* **codec:** implement JSON and XML serialization codecs and event registry ([3bb3c85](https://github.com/wotek/flux/commit/3bb3c85bc92d052732477a797b7e2e629d9b2a10))
* **core:** implement TextMarshaler for Identifier and document JSON compatibility ([1bf935f](https://github.com/wotek/flux/commit/1bf935feb70170f788f534445c85dab34ad49855))

## [2.0.1](https://github.com/wotek/flux/compare/v2.0.0...v2.0.1) (2026-09-22)


### Bug Fixes

* **docs:** remove base path for custom domain deployment ([02cd97a](https://github.com/wotek/flux/commit/02cd97a0ef68d453f11304681a1e895a0f202266))

## [2.0.0](https://github.com/wotek/flux/compare/v1.0.0...v2.0.0) (2026-09-22)


### ⚠ BREAKING CHANGES

* The 'saga' package and all associated types (Saga, SagaStore, etc.) have been fully renamed to 'workflow'. Consumers must update their imports to 'github.com/wotek/flux/workflow' and rename their structs.

### Code Refactoring

* rename Saga to Workflow (breaking API change) ([cfe9525](https://github.com/wotek/flux/commit/cfe95254768984bc49e828ad1d5f54ef650ab918))

## 1.0.0 (2026-09-17)


### Features

* **example/todo:** add interactive terminal CLI client and GetTodoList query ([b4fb411](https://github.com/wotek/flux/commit/b4fb41177f2369158e56058ef7a5c6ac265cccb9))
* **example/todo:** add structured debug logging to server and -debug CLI flag ([2d0590e](https://github.com/wotek/flux/commit/2d0590e301af464638f68e6a93b8632d17cf4b6e))
* **example/todo:** implement server and client packages with standalone binaries ([966e016](https://github.com/wotek/flux/commit/966e016d7922244caaf7ead403ac1922c1aa14d1))
* **example/todo:** implement Server-Sent Events (SSE) and live synchronization for competing clients ([4d6169a](https://github.com/wotek/flux/commit/4d6169ac75ddc8c5117a5041958e96e14b402419))
* **example/todo:** implement two-screen interactive CLI navigation between lists and tasks ([0ab78ab](https://github.com/wotek/flux/commit/0ab78ab82bebae6096a2ebd3a712ca43985ec8c5))
* **example/todo:** integrate Bubble Tea for interactive TUI with list and task cycling ([ba23b51](https://github.com/wotek/flux/commit/ba23b5165e3eff17a04e0526efcb9558729d4256))
* **example/todo:** migrate server HTTP gateway to Echo v4 ([acb2c01](https://github.com/wotek/flux/commit/acb2c01711203ce60312e45115cefef7e33abab1))
* **example/todo:** support multiple todo list aggregates and read-model lists projection ([ed7a05f](https://github.com/wotek/flux/commit/ed7a05f13691ca2827e312038b25d6b5491ae589))
* **example:** implement e-commerce domain application ([c9cf995](https://github.com/wotek/flux/commit/c9cf9958d05bd085e677e7c76c5c2899f9f42cdd))
* **example:** implement todo reference app with isolated struct files ([764bc0e](https://github.com/wotek/flux/commit/764bc0e890cd357facd56be1f5939af10707bc03))
* **flux:** add Use() middleware chaining for commands, queries, and events ([8f35445](https://github.com/wotek/flux/commit/8f35445b6d1eb27fb556ea2284c33a15341875b7))
* **flux:** implement aggregate snapshotting and update event store read with fromRevision ([66f8534](https://github.com/wotek/flux/commit/66f853493440db10ec2ac065a2a383bdba56451c))
* **flux:** inject slog logger into Context with automatic distributed tracing ([695eccd](https://github.com/wotek/flux/commit/695eccd691866a0b708240b64db8fb41f3d69f5c))


### Bug Fixes

* **example/todo:** forward filter messages to list model and clear refresh status ([8cf5f96](https://github.com/wotek/flux/commit/8cf5f96b7141d5b40ccc10d6e848cd3d64d2de63))
* **example/todo:** harden SSE parser and ensure read-model projection convergence on live sync ([8029d7d](https://github.com/wotek/flux/commit/8029d7deef8cc528ced87c8f841d16300e4158a2))
* **flux:** update aggregate revision after successful persistence ([ebc238b](https://github.com/wotek/flux/commit/ebc238b5b1fb50ef213879b10af7610cf5d05199))
