# E-Commerce Application Domain

This document describes the pure business logic, objects, and actions extracted from the e-commerce domain analysis, entirely decoupled from any specific event-sourcing framework.

## Value Objects

- **Address**
  - `ID` (UUID)
  - `Street` (String)
  - `City` (String)
  - `ZipCode` (String)
  - `Country` (String)
- **LineItem**
  - `ProductID` (UUID)
  - `Name` (String)
  - `Price` (Int, in cents)
  - `Quantity` (Int)

---

## Aggregates

### 1. ProductAggregate
Manages the core identity, naming, and inventory of a product.
- **State:**
  - `ID` (UUID)
  - `Name` (String)
  - `Stock` (Int)
- **Commands & Events:**
  - **Create(Name, Stock)** $\rightarrow$ `ProductCreated { Name, Stock }`
  - **Rename(Name)** $\rightarrow$ `ProductRenamed { Name }`
  - **AdjustStock(Quantity)** $\rightarrow$ `StockAdjusted { Quantity }`

### 2. PricingAggregate
Separated from the Product aggregate to handle pricing concerns exclusively, but shares the exact same `ID` as the product it prices.
- **State:**
  - `ID` (UUID - matches Product ID)
  - `Price` (Int, in cents)
- **Commands & Events:**
  - **SetPrice(Price)** $\rightarrow$ `PricingSet { Price }`

### 3. CustomerAggregate
Manages the customer profile and their address book.
- **State:**
  - `ID` (UUID)
  - `Name` (String)
  - `Email` (String)
  - `Addresses` ([]Address)
- **Commands & Events:**
  - **Register(Name, Email)** $\rightarrow$ `CustomerRegistered { Name, Email }`
  - **Rename(Name)** $\rightarrow$ `CustomerRenamed { Name }`
  - **AddAddress(Address)** $\rightarrow$ `AddressAdded { Address }`
  - **RemoveAddress(AddressID)** $\rightarrow$ `AddressRemoved { AddressID }`

### 4. OrderAggregate
Tracks the items, customer association, and lifecycle statuses of a purchase order.
- **State:**
  - `ID` (UUID)
  - `CustomerID` (UUID)
  - `Items` ([]LineItem)
  - `Status` (Enum: Open, Paid, Cancelled)
- **Commands & Events:**
  - **Place(CustomerID, []LineItem)** $\rightarrow$ `OrderPlaced { CustomerID, Items }`
  - **Pay()** $\rightarrow$ `OrderPaid {}`
  - **Cancel(Reason)** $\rightarrow$ `OrderCancelled { Reason }`

---

## Projections (Read Models)

### ProductCatalog
Provides a unified view of products for querying, combining data from both the `ProductAggregate` and `PricingAggregate`.
- **View Model:** `ProductView { ID, Name, Price, Stock }`
- **Handled Events:**
  - `ProductCreated` (Initializes view, sets Name and Stock)
  - `ProductRenamed` (Updates Name)
  - `StockAdjusted` (Updates Stock)
  - `PricingSet` (Updates Price)

---

## Sagas (Workflows)

### PaymentWorkflow
An orchestration process that manages the payment deadline and compensation logic for unpaid or explicitly cancelled orders.
- **Triggered By:** `OrderPlaced`
  - The workflow is keyed by the Order ID.
  - It captures the `LineItems` from the event payload into its internal state for later compensation.
  - It starts a payment deadline timer (e.g., 30 minutes).
- **Process Flow:**
  - **If `OrderPaid` occurs:** The workflow completes successfully.
  - **If `OrderCancelled` occurs (Customer Action):** The workflow intercepts it and enters compensation.
  - **If Deadline Timeout occurs:** The workflow initiates compensation.
- **Compensation Logic (Rollback):**
  - If triggered by a timeout, issue a `CancelOrder` command to the Order aggregate.
  - Await the `OrderCancelled` event confirmation.
  - For each captured `LineItem`, issue an `AdjustStock` command to the respective Product aggregate with a positive quantity to return the reserved stock.
