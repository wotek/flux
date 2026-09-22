package main

import (
	"context"
	"testing"

	"github.com/wotek/flux"
	"github.com/wotek/flux/command"
	eventstore "github.com/wotek/flux/event/store"
)

func TestBankAccount_Lifecycle(t *testing.T) {
	ctx := context.Background()

	eventStore := eventstore.New()
	repo := flux.NewAggregateRepository[*BankAccount, flux.Event](eventStore)

	id := flux.MustParseIdentifier("urn:bank:prod:core:acc123:account:main")
	stream := flux.Stream{Identifier: id}

	actor := flux.Actor{Identifier: flux.MustParseIdentifier("urn:bank:prod:core:user1:user:alice")}
	baseCtx := flux.NewContext(ctx, actor, id, flux.Identifier{})

	cmdBus := command.New()

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

	// Create Account
	cmd1ID := flux.MustParseIdentifier("urn:bank:prod:core:acc123:command:c1")
	cmd1Ctx := command.NewContext(ctx, cmd1ID, actor, id, flux.Identifier{})
	if err := command.Execute(cmd1Ctx, cmdBus, CreateAccountCommand{Owner: "Alice"}); err != nil {
		t.Fatalf("CreateAccountCommand failed: %v", err)
	}

	loaded, err := repo.Load(baseCtx, stream)
	if err != nil {
		t.Fatalf("failed to load account: %v", err)
	}
	if loaded.Owner != "Alice" || loaded.Balance != 0 {
		t.Fatalf("expected Owner: Alice, Balance: 0, got Owner: %s, Balance: %d", loaded.Owner, loaded.Balance)
	}

	// Deposit
	cmd2ID := flux.MustParseIdentifier("urn:bank:prod:core:acc123:command:c2")
	cmd2Ctx := command.NewContext(ctx, cmd2ID, actor, id, cmd1ID)
	if err := command.Execute(cmd2Ctx, cmdBus, DepositCommand{Amount: 250}); err != nil {
		t.Fatalf("DepositCommand failed: %v", err)
	}

	loaded, err = repo.Load(baseCtx, stream)
	if err != nil {
		t.Fatalf("failed to load account: %v", err)
	}
	if loaded.Owner != "Alice" || loaded.Balance != 250 {
		t.Fatalf("expected Owner: Alice, Balance: 250, got Owner: %s, Balance: %d", loaded.Owner, loaded.Balance)
	}
}
