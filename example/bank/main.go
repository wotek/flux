package main

import (
	"context"
	"fmt"
	"log"

	"github.com/wotek/flux"
	"github.com/wotek/flux/command"
	eventstore "github.com/wotek/flux/event/store"
)

// AccountCreated is emitted when a new bank account is opened.
type AccountCreated struct {
	Owner string
}

// Name returns the event name identifier.
func (e AccountCreated) Name() string { return "AccountCreated" }

// MoneyDeposited is emitted when funds are added to an account.
type MoneyDeposited struct {
	Amount int
}

// Name returns the event name identifier.
func (e MoneyDeposited) Name() string { return "MoneyDeposited" }

// CreateAccountCommand instructs the system to open a new bank account.
type CreateAccountCommand struct {
	Owner string
}

// DepositCommand instructs the system to deposit money into a bank account.
type DepositCommand struct {
	Amount int
}

// BankAccount represents a bank account aggregate root.
type BankAccount struct {
	flux.AggregateRoot[flux.Event]
	Owner   string
	Balance int
}

// New creates an uninitialized BankAccount aggregate instance for the given stream.
func (a *BankAccount) New(stream flux.Stream) *BankAccount {
	return NewBankAccount(stream)
}

// NewBankAccount constructs a new BankAccount aggregate root.
func NewBankAccount(stream flux.Stream) *BankAccount {
	a := &BankAccount{}
	a.AggregateRoot = flux.NewAggregateRoot[flux.Event](stream, flux.NewChangeset[flux.Event](), a.apply)
	return a
}

func (a *BankAccount) apply(event flux.Event) error {
	switch e := event.(type) {
	case AccountCreated:
		a.Owner = e.Owner
	case MoneyDeposited:
		a.Balance += e.Amount
	}
	return nil
}

// Create records an AccountCreated event.
func (a *BankAccount) Create(owner string) {
	a.Changeset().Record(AccountCreated{Owner: owner})
}

// Deposit records a MoneyDeposited event.
func (a *BankAccount) Deposit(amount int) {
	a.Changeset().Record(MoneyDeposited{Amount: amount})
}

func main() {
	ctx := context.Background()

	// In-memory event store
	eventStore := eventstore.New()
	repo := flux.NewAggregateRepository[*BankAccount, flux.Event](eventStore)

	// Create stream identifier
	id := flux.MustParseIdentifier("urn:bank:prod:core:acc123:account:main")
	stream := flux.Stream{Identifier: id}

	actor := flux.Actor{Identifier: flux.MustParseIdentifier("urn:bank:prod:core:user1:user:alice")}
	baseCtx := flux.NewContext(ctx, actor, id, flux.Identifier{})

	// Initialize Command Bus
	cmdBus := command.New()

	// Register command handlers
	command.Register(cmdBus, func(ctx command.Context, cmd CreateAccountCommand) error {
		account := NewBankAccount(stream)
		account.Create(cmd.Owner)
		return repo.Save(ctx, account)
	})

	command.Register(cmdBus, func(ctx command.Context, cmd DepositCommand) error {
		account, err := repo.Load(ctx, stream)
		if err != nil {
			return err
		}
		account.Deposit(cmd.Amount)
		return repo.Save(ctx, account)
	})

	// Dispatch CreateAccountCommand
	cmd1ID := flux.MustParseIdentifier("urn:bank:prod:core:acc123:command:c1")
	cmd1Ctx := command.NewContext(ctx, cmd1ID, actor, id, flux.Identifier{})
	if err := command.Execute(cmd1Ctx, cmdBus, CreateAccountCommand{Owner: "Alice"}); err != nil {
		log.Fatal(err)
	}

	// Rehydrate from event stream and verify initial creation
	loaded, err := repo.Load(baseCtx, stream)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Loaded Account: %s, Balance: $%d\n", loaded.Owner, loaded.Balance)

	// Dispatch DepositCommand
	cmd2ID := flux.MustParseIdentifier("urn:bank:prod:core:acc123:command:c2")
	cmd2Ctx := command.NewContext(ctx, cmd2ID, actor, id, cmd1ID)
	if err := command.Execute(cmd2Ctx, cmdBus, DepositCommand{Amount: 250}); err != nil {
		log.Fatal(err)
	}

	// Rehydrate from event stream and verify updated balance
	loaded, err = repo.Load(baseCtx, stream)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Loaded Account: %s, Balance: $%d\n", loaded.Owner, loaded.Balance)
}
