package payment

import (
	"slices"

	"github.com/wotek/flux"
	"github.com/wotek/flux/example/e-commerce/internal/domain/types"
	"github.com/wotek/flux/saga"
)

var _ saga.Saga[*PaymentSaga] = (*PaymentSaga)(nil)

// PaymentSaga manages the payment deadline and compensation state machine for an order.
type PaymentSaga struct {
	ID          flux.Identifier  `json:"id"`
	Items       []types.LineItem `json:"items"`
	IsPaid      bool             `json:"is_paid"`
	IsCancelled bool             `json:"is_cancelled"`
}

// Identifier returns the unique saga instance identifier.
func (s *PaymentSaga) Identifier() flux.Identifier {
	return s.ID
}

// New creates an uninitialized [PaymentSaga] instance.
func (s *PaymentSaga) New() *PaymentSaga {
	return &PaymentSaga{
		Items: make([]types.LineItem, 0),
	}
}

// Clone returns a shallow copy of the saga state with cloned line items.
func (s *PaymentSaga) Clone() *PaymentSaga {
	return &PaymentSaga{
		ID:          s.ID,
		Items:       slices.Clone(s.Items),
		IsPaid:      s.IsPaid,
		IsCancelled: s.IsCancelled,
	}
}
