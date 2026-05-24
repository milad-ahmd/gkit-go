package productv1

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ProductServiceClient is the client-side interface for ProductService.
type ProductServiceClient interface {
	GetProduct(ctx context.Context, req *GetProductRequest, opts ...grpc.CallOption) (*Product, error)
	ListProducts(ctx context.Context, req *ListProductsRequest, opts ...grpc.CallOption) (*ListProductsResponse, error)
	PlaceOrder(ctx context.Context, req *PlaceOrderRequest, opts ...grpc.CallOption) (*PlaceOrderResponse, error)
}

type productServiceClient struct {
	cc grpc.ClientConnInterface
}

// NewProductServiceClient creates a new client stub.
func NewProductServiceClient(cc grpc.ClientConnInterface) ProductServiceClient {
	return &productServiceClient{cc: cc}
}

func (c *productServiceClient) GetProduct(ctx context.Context, req *GetProductRequest, opts ...grpc.CallOption) (*Product, error) {
	out := &Product{}
	err := c.cc.Invoke(ctx, "/"+ServiceName+"/GetProduct", req, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *productServiceClient) ListProducts(ctx context.Context, req *ListProductsRequest, opts ...grpc.CallOption) (*ListProductsResponse, error) {
	out := &ListProductsResponse{}
	err := c.cc.Invoke(ctx, "/"+ServiceName+"/ListProducts", req, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *productServiceClient) PlaceOrder(ctx context.Context, req *PlaceOrderRequest, opts ...grpc.CallOption) (*PlaceOrderResponse, error) {
	out := &PlaceOrderResponse{}
	err := c.cc.Invoke(ctx, "/"+ServiceName+"/PlaceOrder", req, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ---- helpers ------------------------------------------------------------

func errUnimplemented(method string) error {
	return status.Errorf(codes.Unimplemented, "method %s not implemented", method)
}
