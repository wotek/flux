package identity_test

import (
	"testing"

	"github.com/wotek/flux/example/e-commerce/internal/identity"
)

func TestIdentityFactories(t *testing.T) {
	t.Parallel()

	uuid := "550e8400-e29b-41d4-a716-446655440000"

	tests := []struct {
		name         string
		gotURN       string
		expectedType string
		expectedID   string
		expectedURN  string
	}{
		{
			name:         "ProductIdentifier",
			gotURN:       identity.NewProductIdentifier(uuid).String(),
			expectedType: "product",
			expectedID:   uuid,
			expectedURN:  "urn:flux:ecommerce:shop:default:product:" + uuid,
		},
		{
			name:         "PricingIdentifier",
			gotURN:       identity.NewPricingIdentifier(uuid).String(),
			expectedType: "pricing",
			expectedID:   uuid,
			expectedURN:  "urn:flux:ecommerce:shop:default:pricing:" + uuid,
		},
		{
			name:         "CustomerIdentifier",
			gotURN:       identity.NewCustomerIdentifier(uuid).String(),
			expectedType: "customer",
			expectedID:   uuid,
			expectedURN:  "urn:flux:ecommerce:shop:default:customer:" + uuid,
		},
		{
			name:         "OrderIdentifier",
			gotURN:       identity.NewOrderIdentifier(uuid).String(),
			expectedType: "order",
			expectedID:   uuid,
			expectedURN:  "urn:flux:ecommerce:shop:default:order:" + uuid,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if tc.gotURN != tc.expectedURN {
				t.Errorf("expected URN %q, got %q", tc.expectedURN, tc.gotURN)
			}
		})
	}
}
