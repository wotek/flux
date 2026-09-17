package flux_test

import (
	"context"
	"errors"
	"testing"

	"github.com/wotek/flux"
	eventstore "github.com/wotek/flux/event/store"
)

// We can reuse CounterAggregate from aggregate_test.go if we put this in package flux.
// Actually, since we're using flux_test to act as an external consumer, we redefine a simple aggregate.

type BankEvent interface {
	flux.Event
	isBankEvent()
}

type AccountCreated struct {
	Owner string
}

func (e AccountCreated) Name() string { return "AccountCreated" }
func (e AccountCreated) isBankEvent() {}

type MoneyDeposited struct {
	Amount int
}

func (e MoneyDeposited) Name() string { return "MoneyDeposited" }
func (e MoneyDeposited) isBankEvent() {}

type BankAccount struct {
	flux.AggregateRoot[BankEvent]
	Owner   string
	Balance int
}

func (a *BankAccount) New(stream flux.Stream) *BankAccount {
	return NewBankAccount(stream)
}

func NewBankAccount(stream flux.Stream) *BankAccount {
	a := &BankAccount{}
	a.AggregateRoot = flux.NewAggregateRoot[BankEvent](stream, flux.NewChangeset[BankEvent](), a.apply)
	return a
}

func (a *BankAccount) apply(e BankEvent) error {
	switch e := e.(type) {
	case AccountCreated:
		a.Owner = e.Owner
	case MoneyDeposited:
		a.Balance += e.Amount
	}
	return nil
}

func (a *BankAccount) Create(owner string) {
	a.Changeset().Record(AccountCreated{Owner: owner})
	a.apply(AccountCreated{Owner: owner})
}

func (a *BankAccount) Deposit(amount int) {
	a.Changeset().Record(MoneyDeposited{Amount: amount})
	a.apply(MoneyDeposited{Amount: amount})
}

func TestAggregateRepository_SaveAndLoad(t *testing.T) {
	ctx := flux.NewContext(context.Background(), flux.Actor{}, flux.Identifier{}, flux.Identifier{})
	store := eventstore.New()
	repo := flux.NewAggregateRepository[*BankAccount, BankEvent](store)

	id := flux.NewIdentifierFromString("urn:bank:prod:accounts:123:account:abc-999")
	stream := flux.Stream{Identifier: id}
	account := NewBankAccount(stream)

	account.Create("Alice")
	account.Deposit(100)
	account.Deposit(50)

	// Save to in-memory event store
	if err := repo.Save(ctx, account); err != nil {
		t.Fatalf("failed to save aggregate: %v", err)
	}

	if account.Revision() != 3 {
		t.Errorf("expected original aggregate instance to have revision 3 after save, got %d", account.Revision())
	}

	if account.Changeset().HasChanges() {
		t.Errorf("expected changeset to be cleared after save")
	}

	// Load it back
	loaded, err := repo.Load(ctx, stream)
	if err != nil {
		t.Fatalf("failed to load aggregate: %v", err)
	}

	if loaded.Owner != "Alice" {
		t.Errorf("expected owner Alice, got %s", loaded.Owner)
	}
	if loaded.Balance != 150 {
		t.Errorf("expected balance 150, got %d", loaded.Balance)
	}
	if loaded.Revision() != 3 {
		t.Errorf("expected revision 3, got %d", loaded.Revision())
	}
}

func TestAggregateRepository_ConcurrencyError(t *testing.T) {
	ctx := flux.NewContext(context.Background(), flux.Actor{}, flux.Identifier{}, flux.Identifier{})
	store := eventstore.New()
	repo := flux.NewAggregateRepository[*BankAccount, BankEvent](store)

	id := flux.NewIdentifierFromString("urn:bank:prod:accounts:123:account:abc-999")
	stream := flux.Stream{Identifier: id}
	account1 := NewBankAccount(stream)
	account1.Create("Alice")
	if err := repo.Save(ctx, account1); err != nil {
		t.Fatalf("failed to save account1: %v", err)
	}

	// Load two instances to simulate concurrency
	aggA, errA := repo.Load(ctx, stream)
	aggB, errB := repo.Load(ctx, stream)
	if errA != nil || errB != nil {
		t.Fatalf("failed to load aggA/aggB: %v %v", errA, errB)
	}

	aggA.Deposit(10)
	aggB.Deposit(20)

	// aggA saves successfully
	if err := repo.Save(ctx, aggA); err != nil {
		t.Fatalf("unexpected error saving aggA: %v", err)
	}

	// aggB should fail with optimistic concurrency
	err := repo.Save(ctx, aggB)
	if err == nil {
		t.Fatalf("expected concurrency error when saving aggB, but got nil")
	}
	if !errors.Is(err, flux.ErrConcurrency) {
		t.Fatalf("expected ErrConcurrency, got %v", err)
	}
}

func TestAggregateRepository_NotFound(t *testing.T) {
	ctx := flux.NewContext(context.Background(), flux.Actor{}, flux.Identifier{}, flux.Identifier{})
	store := eventstore.New()
	repo := flux.NewAggregateRepository[*BankAccount, BankEvent](store)

	id := flux.NewIdentifierFromString("urn:bank:prod:accounts:123:account:does-not-exist")
	stream := flux.Stream{Identifier: id}

	_, err := repo.Load(ctx, stream)
	if err == nil {
		t.Fatalf("expected error loading non-existent aggregate, got nil")
	}
	if !errors.Is(err, flux.ErrAggregateNotFound) {
		t.Fatalf("expected ErrAggregateNotFound, got %v", err)
	}
}
