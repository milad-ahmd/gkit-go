package validation_test

import (
	"errors"
	"testing"

	"github.com/milad-ahmd/gkit-go/pkg/validation"
)

var v = validation.New()

type order struct {
	ProductID string `json:"product_id" validate:"required"`
	Quantity  int    `json:"quantity"   validate:"required,min=1,max=1000"`
	Email     string `json:"email"      validate:"required,email"`
	Status    string `json:"status"     validate:"oneof=pending|active|cancelled"`
	Note      string `json:"note"       validate:"max=500"`
	Code      string `json:"code"       validate:"regex=^[A-Z]{3}[0-9]{3}$"`
}

func TestValidate_Valid(t *testing.T) {
	req := order{
		ProductID: "p1",
		Quantity:  5,
		Email:     "user@example.com",
		Status:    "pending",
		Code:      "ABC123",
	}
	if err := v.Validate(&req); err != nil {
		t.Errorf("expected nil, got %v", err)
	}
}

func TestValidate_Required(t *testing.T) {
	req := order{Quantity: 1, Email: "a@b.com", Status: "pending"}
	err := v.Validate(&req)
	assertField(t, err, "product_id")
}

func TestValidate_MinMax(t *testing.T) {
	req := order{ProductID: "p1", Quantity: 0, Email: "a@b.com"}
	err := v.Validate(&req)
	assertField(t, err, "quantity")

	req.Quantity = 1001
	err = v.Validate(&req)
	assertField(t, err, "quantity")
}

func TestValidate_Email(t *testing.T) {
	req := order{ProductID: "p1", Quantity: 1, Email: "not-an-email"}
	err := v.Validate(&req)
	assertField(t, err, "email")
}

func TestValidate_OneOf(t *testing.T) {
	req := order{ProductID: "p1", Quantity: 1, Email: "a@b.com", Status: "unknown"}
	err := v.Validate(&req)
	assertField(t, err, "status")
}

func TestValidate_Regex(t *testing.T) {
	req := order{ProductID: "p1", Quantity: 1, Email: "a@b.com", Code: "bad"}
	err := v.Validate(&req)
	assertField(t, err, "code")
}

func TestValidate_MultipleErrors(t *testing.T) {
	req := order{} // everything missing / zero
	err := v.Validate(&req)
	if err == nil {
		t.Fatal("expected errors")
	}
	var ve *validation.Error
	if !errors.As(err, &ve) {
		t.Fatalf("expected *validation.Error, got %T", err)
	}
	if len(ve.Fields) < 2 {
		t.Errorf("expected multiple field errors, got %v", ve.Fields)
	}
}

func assertField(t *testing.T, err error, field string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error for field %q, got nil", field)
	}
	var ve *validation.Error
	if !errors.As(err, &ve) {
		t.Fatalf("expected *validation.Error, got %T: %v", err, err)
	}
	if _, ok := ve.Fields[field]; !ok {
		t.Errorf("expected error for field %q, got fields %v", field, ve.Fields)
	}
}
