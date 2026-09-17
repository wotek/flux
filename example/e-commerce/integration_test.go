package ecommerce_test

import (
	"context"
	"testing"
	"time"

	"github.com/wotek/flux"
	"github.com/wotek/flux/command"
	eventstore "github.com/wotek/flux/event/store"
	ecommercecmd "github.com/wotek/flux/example/e-commerce/internal/command"
	"github.com/wotek/flux/example/e-commerce/internal/domain/customer"
	"github.com/wotek/flux/example/e-commerce/internal/domain/order"
	"github.com/wotek/flux/example/e-commerce/internal/domain/pricing"
	"github.com/wotek/flux/example/e-commerce/internal/domain/product"
	"github.com/wotek/flux/example/e-commerce/internal/domain/types"
	"github.com/wotek/flux/example/e-commerce/internal/identity"
	"github.com/wotek/flux/example/e-commerce/internal/projection/catalog"
	"github.com/wotek/flux/example/e-commerce/internal/saga/payment"
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

	productRepo := flux.NewAggregateRepository[*product.ProductAggregate, product.ProductEvent](es)
	pricingRepo := flux.NewAggregateRepository[*pricing.PricingAggregate, pricing.PricingEvent](es)
	customerRepo := flux.NewAggregateRepository[*customer.CustomerAggregate, customer.CustomerEvent](es)
	orderRepo := flux.NewAggregateRepository[*order.OrderAggregate, order.OrderEvent](es)

	// 2. Command Handlers
	ecommercecmd.RegisterHandlers(cmdBus, productRepo, pricingRepo, customerRepo, orderRepo)

	// 3. Projections
	catStore := catalog.NewMemoryCatalogStore()
	catalogProjID := flux.NewIdentifierFromString("urn:flux:ecommerce:shop:default:projection:catalog")
	catalogProjector := catalog.NewCatalogProjector(catalogProjID, es, ps, catStore)
	go func() {
		_ = catalogProjector.Start(ctx)
	}()

	// 4. Sagas
	paymentSagaStore := sagastore.New[*payment.PaymentSaga](cmdBus)
	paymentSagaStore.StartRelay(ctx)
	orchestrator := saga.NewOrchestrator(es)
	payment.RegisterPaymentSaga(orchestrator, paymentSagaStore)
	go func() {
		_ = orchestrator.Start(ctx)
	}()

	actor := flux.Actor{Identifier: flux.NewIdentifierFromString("urn:flux:ecommerce:shop:default:user:buyer")}

	// -------------------------------------------------------------
	// Scenario A: Product Creation and Pricing
	// -------------------------------------------------------------
	prodID := "item-laptop-1"
	pCmdCtx := command.NewContext(ctx, flux.NewIdentifierFromString("urn:flux:ecommerce:shop:default:command:c1"), actor, identity.NewProductIdentifier(prodID), flux.Identifier{})

	if err := command.Execute(pCmdCtx, cmdBus, ecommercecmd.CreateProduct{
		ProductID: prodID,
		Name:      "MacBook Pro",
		Stock:     10,
	}); err != nil {
		t.Fatalf("CreateProduct failed: %v", err)
	}

	if err := command.Execute(pCmdCtx, cmdBus, ecommercecmd.SetPrice{
		ProductID: prodID,
		Price:     249900,
	}); err != nil {
		t.Fatalf("SetPrice failed: %v", err)
	}

	// Verify catalog projection converged
	var catView catalog.ProductView
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
	// Scenario B: Customer Registration and Address Management
	// -------------------------------------------------------------
	custID := "cust-alice"
	cCmdCtx := command.NewContext(ctx, flux.NewIdentifierFromString("urn:flux:ecommerce:shop:default:command:c2"), actor, identity.NewCustomerIdentifier(custID), flux.Identifier{})

	if err := command.Execute(cCmdCtx, cmdBus, ecommercecmd.RegisterCustomer{
		CustomerID: custID,
		Name:       "Alice Smith",
		Email:      "alice@example.com",
	}); err != nil {
		t.Fatalf("RegisterCustomer failed: %v", err)
	}

	if err := command.Execute(cCmdCtx, cmdBus, ecommercecmd.AddAddress{
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
	// Scenario C: Order Placement & Inventory Reservation
	// -------------------------------------------------------------
	orderID := "ord-12345"
	orderURN := identity.NewOrderIdentifier(orderID)

	// Reserve stock: decrease stock by 2 for the order
	if err := command.Execute(pCmdCtx, cmdBus, ecommercecmd.AdjustStock{
		ProductID: prodID,
		Quantity:  -2,
	}); err != nil {
		t.Fatalf("AdjustStock failed: %v", err)
	}

	// Place order
	oCmdCtx := command.NewContext(ctx, flux.NewIdentifierFromString("urn:flux:ecommerce:shop:default:command:c3"), actor, orderURN, flux.Identifier{})
	if err := command.Execute(oCmdCtx, cmdBus, ecommercecmd.PlaceOrder{
		OrderID:    orderID,
		CustomerID: custID,
		Items: []types.LineItem{
			{ProductID: prodID, Name: "MacBook Pro", Price: 249900, Quantity: 2},
		},
	}); err != nil {
		t.Fatalf("PlaceOrder failed: %v", err)
	}

	// -------------------------------------------------------------
	// Scenario D: Payment Timeout triggers Saga Compensation
	// -------------------------------------------------------------
	// Append PaymentTimeout event on a timer stream correlated with the order URN
	timerStream := flux.Stream{Identifier: flux.NewIdentifierFromString("urn:flux:ecommerce:shop:default:timer:timeout-" + orderID)}
	_ = es.Append(ctx, timerStream, 0, []flux.Envelope{
		{
			Identifier:            flux.NewIdentifierFromString("urn:flux:ecommerce:shop:default:event:timeout-" + orderID),
			Stream:                timerStream,
			Revision:              1,
			Event:                 payment.PaymentTimeout{OrderID: orderID},
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
		if err == nil && ord.Status() == order.OrderStatusCancelled {
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
