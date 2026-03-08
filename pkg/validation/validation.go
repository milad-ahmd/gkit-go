// Package validation provides struct validation using field tags.
//
// Supported rules (via `validate` struct tag, comma-separated):
//
//	required          — field must be non-zero
//	min=N             — string/slice len ≥ N; number ≥ N
//	max=N             — string/slice len ≤ N; number ≤ N
//	email             — must contain @ and a dot after @
//	url               — must parse as an absolute URL
//	oneof=a|b|c       — string must be one of the listed values
//	regex=^[a-z]+$    — must match the given regular expression
//
// # Usage
//
//	type CreateOrderRequest struct {
//	    ProductID string  `json:"product_id" validate:"required"`
//	    Quantity  int     `json:"quantity"   validate:"required,min=1,max=1000"`
//	    Email     string  `json:"email"      validate:"required,email"`
//	    Status    string  `json:"status"     validate:"oneof=pending|active|cancelled"`
//	}
//
//	v := validation.New()
//	if err := v.Validate(&req); err != nil {
//	    var ve *validation.Error
//	    if errors.As(err, &ve) {
//	        // ve.Fields maps field names to rule failure messages.
//	        respondJSON(w, 422, ve)
//	        return
//	    }
//	}
package validation

import (
	"fmt"
	"net/url"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"
)

// Validator validates structs based on field tags.
type Validator struct{}

// New creates a Validator.
func New() *Validator { return &Validator{} }

// Validate validates dst (must be a pointer to a struct).
// Returns *Error if any field fails validation, nil on success.
func (v *Validator) Validate(dst any) error {
	rv := reflect.ValueOf(dst)
	if rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
	}
	if rv.Kind() != reflect.Struct {
		return fmt.Errorf("validation: expected struct, got %T", dst)
	}
	return validateStruct(rv)
}

// --------------------------------------------------------------------------
// Error type

// Error carries per-field validation failures.
type Error struct {
	Fields map[string][]string `json:"fields"` // field name → list of rule messages
}

func (e *Error) Error() string {
	var b strings.Builder
	for field, msgs := range e.Fields {
		fmt.Fprintf(&b, "%s: %s; ", field, strings.Join(msgs, ", "))
	}
	return strings.TrimSuffix(b.String(), "; ")
}

func newVErr() *Error { return &Error{Fields: make(map[string][]string)} }

func (e *Error) add(field, msg string) { e.Fields[field] = append(e.Fields[field], msg) }

func (e *Error) empty() bool { return len(e.Fields) == 0 }

// --------------------------------------------------------------------------
// Core

func validateStruct(rv reflect.Value) error {
	rt := rv.Type()
	ve := newVErr()

	for i := range rt.NumField() {
		field := rt.Field(i)
		fv := rv.Field(i)

		// Recurse into nested structs.
		if fv.Kind() == reflect.Struct && field.Tag.Get("validate") == "" {
			if err := validateStruct(fv); err != nil {
				if nested, ok := err.(*Error); ok {
					for k, msgs := range nested.Fields {
						ve.Fields[field.Name+"."+k] = msgs
					}
				}
			}
			continue
		}

		tag := field.Tag.Get("validate")
		if tag == "" {
			continue
		}

		// Use json tag name if available, else field name.
		name := field.Name
		if j := field.Tag.Get("json"); j != "" {
			name = strings.Split(j, ",")[0]
		}

		rules := strings.Split(tag, ",")
		for _, rule := range rules {
			rule = strings.TrimSpace(rule)
			if msg := applyRule(rule, fv, field.Type); msg != "" {
				ve.add(name, msg)
			}
		}
	}

	if ve.empty() {
		return nil
	}
	return ve
}

func applyRule(rule string, fv reflect.Value, ft reflect.Type) string {
	switch {
	case rule == "required":
		return checkRequired(fv)

	case rule == "email":
		s, ok := toString(fv)
		if !ok || s == "" {
			return ""
		}
		return checkEmail(s)

	case rule == "url":
		s, ok := toString(fv)
		if !ok || s == "" {
			return ""
		}
		return checkURL(s)

	case strings.HasPrefix(rule, "min="):
		n, err := strconv.ParseFloat(strings.TrimPrefix(rule, "min="), 64)
		if err != nil {
			return ""
		}
		return checkMin(fv, n)

	case strings.HasPrefix(rule, "max="):
		n, err := strconv.ParseFloat(strings.TrimPrefix(rule, "max="), 64)
		if err != nil {
			return ""
		}
		return checkMax(fv, n)

	case strings.HasPrefix(rule, "oneof="):
		options := strings.Split(strings.TrimPrefix(rule, "oneof="), "|")
		s, ok := toString(fv)
		if !ok || s == "" {
			return ""
		}
		return checkOneOf(s, options)

	case strings.HasPrefix(rule, "regex="):
		pattern := strings.TrimPrefix(rule, "regex=")
		s, ok := toString(fv)
		if !ok || s == "" {
			return ""
		}
		return checkRegex(s, pattern)
	}

	return ""
}

// --------------------------------------------------------------------------
// Rule implementations

func checkRequired(fv reflect.Value) string {
	switch fv.Kind() {
	case reflect.String:
		if fv.String() == "" {
			return "is required"
		}
	case reflect.Slice, reflect.Map, reflect.Array:
		if fv.Len() == 0 {
			return "is required"
		}
	case reflect.Ptr, reflect.Interface:
		if fv.IsNil() {
			return "is required"
		}
	default:
		if fv.IsZero() {
			return "is required"
		}
	}
	return ""
}

func checkMin(fv reflect.Value, n float64) string {
	switch fv.Kind() {
	case reflect.String:
		if float64(utf8.RuneCountInString(fv.String())) < n {
			return fmt.Sprintf("must be at least %v characters", n)
		}
	case reflect.Slice, reflect.Array, reflect.Map:
		if float64(fv.Len()) < n {
			return fmt.Sprintf("must have at least %v items", n)
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if float64(fv.Int()) < n {
			return fmt.Sprintf("must be ≥ %v", n)
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if float64(fv.Uint()) < n {
			return fmt.Sprintf("must be ≥ %v", n)
		}
	case reflect.Float32, reflect.Float64:
		if fv.Float() < n {
			return fmt.Sprintf("must be ≥ %v", n)
		}
	}
	return ""
}

func checkMax(fv reflect.Value, n float64) string {
	switch fv.Kind() {
	case reflect.String:
		if float64(utf8.RuneCountInString(fv.String())) > n {
			return fmt.Sprintf("must be at most %v characters", n)
		}
	case reflect.Slice, reflect.Array, reflect.Map:
		if float64(fv.Len()) > n {
			return fmt.Sprintf("must have at most %v items", n)
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if float64(fv.Int()) > n {
			return fmt.Sprintf("must be ≤ %v", n)
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if float64(fv.Uint()) > n {
			return fmt.Sprintf("must be ≤ %v", n)
		}
	case reflect.Float32, reflect.Float64:
		if fv.Float() > n {
			return fmt.Sprintf("must be ≤ %v", n)
		}
	}
	return ""
}

func checkEmail(s string) string {
	at := strings.LastIndex(s, "@")
	if at < 1 || at >= len(s)-3 {
		return "must be a valid email address"
	}
	domain := s[at+1:]
	if !strings.Contains(domain, ".") {
		return "must be a valid email address"
	}
	return ""
}

func checkURL(s string) string {
	u, err := url.ParseRequestURI(s)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return "must be a valid URL"
	}
	return ""
}

func checkOneOf(s string, options []string) string {
	for _, o := range options {
		if s == o {
			return ""
		}
	}
	return fmt.Sprintf("must be one of [%s]", strings.Join(options, ", "))
}

func checkRegex(s, pattern string) string {
	re, err := regexp.Compile(pattern)
	if err != nil {
		return fmt.Sprintf("invalid regex pattern: %v", err)
	}
	if !re.MatchString(s) {
		return fmt.Sprintf("must match pattern %q", pattern)
	}
	return ""
}

func toString(fv reflect.Value) (string, bool) {
	if fv.Kind() == reflect.String {
		return fv.String(), true
	}
	return "", false
}
