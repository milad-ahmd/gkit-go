// Package productv1 defines the Product service API types and gRPC bindings.
//
// The .proto definition lives at api/product/v1/product.proto.
// To regenerate the protobuf code (requires protoc + plugins):
//
//	make proto
//
// This file contains the equivalent Go types with JSON tags, allowing the
// service to be used with the JSON gRPC codec without a protoc dependency.
package productv1

// Product represents a single item in the catalog.
type Product struct {
	ID    string  `json:"id"`
	Name  string  `json:"name"`
	Price float64 `json:"price"`
}

// GetProductRequest is the request message for ProductService.GetProduct.
type GetProductRequest struct {
	ID string `json:"id"`
}

// ListProductsRequest is the request message for ProductService.ListProducts.
type ListProductsRequest struct {
	PageSize  int32  `json:"page_size"`
	PageToken string `json:"page_token"`
}

// ListProductsResponse is the response message for ProductService.ListProducts.
type ListProductsResponse struct {
	Products      []*Product `json:"products"`
	NextPageToken string     `json:"next_page_token,omitempty"`
}

// PlaceOrderRequest is the request message for ProductService.PlaceOrder.
type PlaceOrderRequest struct {
	ProductID string `json:"product_id"`
	Quantity  int32  `json:"quantity"`
}

// PlaceOrderResponse is the response message for ProductService.PlaceOrder.
type PlaceOrderResponse struct {
	OrderID string `json:"order_id"`
	Status  string `json:"status"`
}
