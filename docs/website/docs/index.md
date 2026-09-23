---
layout: home

hero:
  name: "flux"
  text: "Event-Sourcing Framework for Go"
  tagline: Build distributed, event-sourced applications with type-safe aggregates, commands, and projections.
  image:
    src: /logo.png
    alt: flux logo
  actions:
    - theme: brand
      text: Start Tutorial
      link: /tutorial/01-project-setup
    - theme: alt
      text: Quick Start
      link: /getting-started/quick-start
    - theme: alt
      text: GitHub
      link: https://github.com/wotek/flux

features:
  - title: Event-Sourced Aggregates
    details: Embed a base type, register typed event handlers, and let the framework handle versioning, persistence, and replay.
  - title: Reflection-Free Generics
    details: Fully utilizes Go 1.20+ Generics for type-safe, blazing fast execution without the overhead of reflection.
  - title: Type-Safe Commands
    details: Dispatch and handle commands with full generic type safety. Cleanly separate your read and write models.
  - title: Projection Toolkit
    details: Build read models with continuous tailing. Decouple your write-path from your read-path entirely.
  - title: Durable Workflows
    details: Coordinate long-running processes across services with Temporal integration—durable commands, timeouts, and explicit compensation.
  - title: Batteries Included
    details: In-memory backends for testing ready to go. Easily swap to MySQL or Redis for production workloads.
---
