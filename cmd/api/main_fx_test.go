package main

import (
	"testing"

	"go.uber.org/fx"
)

func TestApplicationFxComposition(t *testing.T) {
	if err := fx.ValidateApp(AppOptions()); err != nil {
		t.Fatalf("Fx application graph is invalid: %v", err)
	}
}
