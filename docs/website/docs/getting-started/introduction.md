# Introduction

Welcome to the `flux` framework documentation!

`flux` is a lightweight, high-performance, and strictly-typed Event Sourcing & CQRS framework for Go 1.27+. It is designed to provide you with the architectural benefits of distributed event sourcing without the immense boilerplate usually associated with it.

## Why flux?

Most Go event-sourcing libraries rely heavily on `reflect` to decode events, map handlers, and hydrate aggregates. This introduces significant runtime performance penalties and completely bypasses the compiler's type safety.

`flux` was built from the ground up to leverage **Go 1.20+ Generics**. 

By utilizing strict generic constraints on Aggregates and explicitly typed decoding mechanisms, `flux` ensures that if your event-sourced application compiles, it is fundamentally sound.

## Core Principles

1. **Reflection-Free:** Generics are used everywhere. Your command handlers, queries, and aggregates are statically typed.
2. **Framework Decoupling:** Business logic is entirely isolated. Aggregates have zero knowledge of databases, Event Stores, or network boundaries.
3. **Domain Purity (Serialization Agnostic):** The core domain is entirely free of infrastructure pollution. You will never write a single `json:` or `xml:` tag on your core Domain Events. Serialization is handled exclusively at the edges via DTOs, generic Type Registries, and "Batteries-Included" Codecs.
4. **Bring Your Own Backend:** `flux` ships with a blazing-fast In-Memory testing backend, but allows you to seamlessly swap to MySQL, Redis, or PostgreSQL via a clean `EventStore` interface.

## Next Steps

Head over to the [Installation](/getting-started/installation) guide to add `flux` to your Go module, and then dive into the [Quick Start](/getting-started/quick-start) to build your first event-sourced Todo application!
