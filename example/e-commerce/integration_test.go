package ecommerce_test

import (
	"context"
	"testing"
	"time"

	"github.com/wotek/flux"
	"github.com/wotek/flux/command"
	eventstore "github.com/wotek/flux/event/store"
	pricingagg "github.com/wotek/flux/example/e-commerce/internal/catalog/aggregates/pricing"
	productagg "github.com/wotek/flux/example/e-commerce/internal/catalog/aggregates/product"
	catalogcmd "github.com/wotek/flux/example/e-commerce/internal/catalog/commands"
	catalogevents "github.com/wotek/flux/example/e-commerce/internal/catalog/events"
	catalogproj "github.com/wotek/flux/example/e-commerce/internal/catalog/projections"
	"github.com/wotek/flux/example/e-commerce/internal/identity"
	customeragg "github.com/wotek/flux/example/e-commerce/internal/sales/aggregates/customer"
	orderagg "github.com/wotek/flux/example/e-commerce/internal/sales/aggregates/order"
	salescmd "github.com/wotek/flux/example/e-commerce/internal/sales/commands"
	salesevents "github.com/wotek/flux/example/e-commerce/internal/sales/events"
	salestypes "github.com/wotek/flux/example/e-commerce/internal/sales/types"
	"github.com/wotek/flux/example/e-commerce/internal/types"
	paymentwf "github.com/wotek/flux/example/e-commerce/internal/workflows/payment"
	projectionstore "github.com/wotek/flux/projection/store"
	"github.com/wotek/flux/saga"
	sagastore "github.com/wotek/flux/saga/store"
)

func TestEcommerce_EndToEnd_LifecycleAndCompensation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 1. Infrastructure
	cmdBus := command.New()
	es := eventstore.New()
	ps := projectionstore.New()

	productRepo := flux.NewAggregateRepository[*productagg.ProductAggregate, catalogevents.ProductEvent](es)
	pricingRepo := flux.NewAggregateRepository[*pricingagg.PricingAggregate, catalogevents.PricingEvent](es)
	customerRepo := flux.NewAggregateRepository[*customeragg.CustomerAggregate, salesevents.CustomerEvent](es)
	orderRepo := flux.NewAggregateRepository[*orderagg.OrderAggregate, salesevents.OrderEvent](es)

	// 2. Command Handlers
	catalogcmd.RegisterHandlers(cmdBus, productRepo, pricingRepo)
	salescmd.RegisterHandlers(cmdBus, customerRepo, orderRepo)

	// 3. Projections
	catStore := catalogproj.NewMemoryStore()
	catalogProjID := flux.NewIdentifierFromString("urn:flux:ecommerce:shop:default:projection:catalog")
	catalogProjector := catalogproj.NewProductCatalogProjector(catalogProjID, es, ps, catStore)
	go func() {
		_ = catalogProjector.Start(ctx)
	}()

	// 4. Sagas / Workflows
	paymentSagaStore := sagastore.New[*paymentwf.PaymentSaga](cmdBus)
	paymentSagaStore.StartRelay(ctx)
	orchestrator := saga.NewOrchestrator(es)
	paymentwf.RegisterPaymentSaga(orchestrator, paymentSagaStore)
	go func() {
		_ = orchestrator.Start(ctx)
	}()

	actor := flux.Actor{Identifier: flux.NewIdentifierFromString("urn:flux:ecommerce:shop:default:user:buyer")}

	// -------------------------------------------------------------
	// Scenario A: Product Creation and Pricing (Catalog Domain)
	// -------------------------------------------------------------
	prodID := "item-laptop-1"
	pCmdCtx := command.NewContext(ctx, flux.NewIdentifierFromString("urn:flux:ecommerce:shop:default:command:c1"), actor, identity.NewProductIdentifier(prodID), flux.Identifier{})

	if err := command.Execute(pCmdCtx, cmdBus, catalogcmd.CreateProduct{
		ProductID: prodID,
		Name:      "MacBook Pro",
		Stock:     10,
	}); err != nil {
		t.Fatalf("CreateProduct failed: %v", err)
	}

	if err := command.Execute(pCmdCtx, cmdBus, catalogcmd.SetPrice{
		ProductID: prodID,
		Price:     249900,
	}); err != nil {
		t.Fatalf("SetPrice failed: %v", err)
	}

	// Verify catalog projection converged
	var catView catalogproj.ProductView
	var found bool
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		catView, found, _ = catStore.Get(ctx, prodID)
		if found && catView.Name == "MacBook Pro" && catView.Price == 249900 && catView.Stock == 10 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !found || catView.Price != 249900 || catView.Stock != 10 {
		t.Fatalf("catalog projection failed to converge: %+v", catView)
	}

	// -------------------------------------------------------------
	// Scenario B: Customer Registration and Address Management (Sales Domain)
	// -------------------------------------------------------------
	custID := "cust-alice"
	cCmdCtx := command.NewContext(ctx, flux.NewIdentifierFromString("urn:flux:ecommerce:shop:default:command:c2"), actor, identity.NewCustomerIdentifier(custID), flux.Identifier{})

	if err := command.Execute(cCmdCtx, cmdBus, salescmd.RegisterCustomer{
		CustomerID: custID,
		Name:       "Alice Smith",
		Email:      "alice@example.com",
	}); err != nil {
		t.Fatalf("RegisterCustomer failed: %v", err)
	}

	if err := command.Execute(cCmdCtx, cmdBus, salescmd.AddAddress{
		CustomerID: custID,
		Address: types.Address{
			ID:      "addr-1",
			Street:  "123 Market St",
			City:    "San Francisco",
			ZipCode: "94103",
			Country: "USA",
		},
	}); err != nil {
		t.Fatalf("AddAddress failed: %v", err)
	}

	// -------------------------------------------------------------
	// Scenario C: Order Placement & Inventory Reservation (Sales + Catalog)
	// -------------------------------------------------------------
	orderID := "ord-12345"
	orderURN := identity.NewOrderIdentifier(orderID)

	// Reserve stock: decrease stock by 2 for the order
	if err := command.Execute(pCmdCtx, cmdBus, catalogcmd.AdjustStock{
		ProductID: prodID,
		Quantity:  -2,
	}); err != nil {
		t.Fatalf("AdjustStock failed: %v", err)
	}

	// Place order
	oCmdCtx := command.NewContext(ctx, flux.NewIdentifierFromString("urn:flux:ecommerce:shop:default:command:c3"), actor, orderURN, flux.Identifier{})
	if err := command.Execute(oCmdCtx, cmdBus, salescmd.PlaceOrder{
		OrderID:    orderID,
		CustomerID: custID,
		Items: []salestypes.LineItem{
			{ProductID: prodID, Name: "MacBook Pro", Price: 249900, Quantity: 2},
		},
	}); err != nil {
		t.Fatalf("PlaceOrder failed: %v", err)
	}

	// -------------------------------------------------------------
	// Scenario D: Payment Timeout triggers Workflow / Saga Compensation
	// -------------------------------------------------------------
	// Append PaymentTimeout event on a timer stream correlated with the order URN
	timerStream := flux.Stream{Identifier: flux.NewIdentifierFromString("urn:flux:ecommerce:shop:default:timer:timeout-" + orderID)}
	_ = es.Append(ctx, timerStream, 0, []flux.Envelope{
		{
			Identifier:            flux.NewIdentifierFromString("urn:flux:ecommerce:shop:default:event:timeout-" + orderID),
			Stream:                timerStream,
			Revision:              1,
			Event:                 paymentwf.PaymentTimeout{OrderID: orderID},
			Actor:                 actor,
			CorrelationIdentifier: orderURN,
			CreatedAt:             time.Now(),
		},
	})

	// Await compensation:
	// 1. Order becomes Cancelled
	// 2. Product stock gets compensated back (+2) to 10
	deadline = time.Now().Add(3 * time.Second)
	var finalStock int
	var orderCancelled bool

	fluxCtx := flux.NewContext(ctx, actor, flux.Identifier{}, flux.Identifier{})
	orderStream := flux.Stream{Identifier: orderURN}
	for time.Now().Before(deadline) {
		ord, err := orderRepo.Load(fluxCtx, orderStream)
		if err == nil && ord.Status() == salestypes.OrderStatusCancelled {
			orderCancelled = true
		}

		prod, err := productRepo.Load(fluxCtx, flux.Stream{Identifier: identity.NewProductIdentifier(prodID)})
		if err == nil {
			finalStock = prod.Stock()
		}

		if orderCancelled && finalStock == 10 {
			break
		}
		time.Sleep(30 * time.Millisecond)
	}

	if !orderCancelled {
		t.Errorf("expected order to be cancelled by payment timeout compensation")
	}
	if finalStock != 10 {
		t.Errorf("expected stock to be restored to 10, got %d", finalStock)
	}
}
