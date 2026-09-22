# 1. Project Setup

Welcome to the comprehensive E-Commerce Tutorial! Over the next few chapters, we will build a production-ready, event-sourced E-Commerce backend.

Unlike the simple Todo app, a real system requires strict boundaries. We will build this application using **Domain-Driven Design (DDD)** principles, isolating our business logic into multiple Bounded Contexts.

## Our Domains

Our e-commerce system will be split into the following domains:

1. **Catalog Domain:** Responsible for managing Products, Inventory, and Pricing.
2. **Sales Domain:** Responsible for managing Customers, Carts, and Orders.
3. **Workflows:** Responsible for long-running, cross-domain process managers (like waiting for a Payment to clear before shipping an Order).

## Initializing the Workspace

Let's create our workspace and initialize the Go module.

```bash
mkdir e-commerce && cd e-commerce
go mod init github.com/yourusername/e-commerce
go get github.com/wotek/flux
```

## Directory Structure

We will adopt a standard Go project layout:

```text
e-commerce/
├── cmd/
│   └── server/          # Application entrypoint (main.go)
├── internal/
│   ├── catalog/         # Catalog bounded context
│   │   ├── aggregates/  # Product, Pricing
│   │   ├── commands/    
│   │   └── events/      
│   ├── sales/           # Sales bounded context
│   │   ├── aggregates/  # Order, Customer
│   │   ├── commands/    
│   │   └── events/      
│   └── workflows/       # Payment process manager
```

Go ahead and scaffold these directories. In the next chapter, we will build our first complex aggregate: **The Product**.
