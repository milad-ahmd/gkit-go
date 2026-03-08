package productv1

import (
	"context"

	"google.golang.org/grpc"
)

// ServiceName is the fully-qualified gRPC service name.
const ServiceName = "product.v1.ProductService"

// ProductServiceServer is the server-side interface to implement.
type ProductServiceServer interface {
	GetProduct(ctx context.Context, req *GetProductRequest) (*Product, error)
	ListProducts(ctx context.Context, req *ListProductsRequest) (*ListProductsResponse, error)
	PlaceOrder(ctx context.Context, req *PlaceOrderRequest) (*PlaceOrderResponse, error)
}

// UnimplementedProductServiceServer can be embedded to satisfy future interface additions.
type UnimplementedProductServiceServer struct{}

func (UnimplementedProductServiceServer) GetProduct(_ context.Context, _ *GetProductRequest) (*Product, error) {
	return nil, errUnimplemented("GetProduct")
}
func (UnimplementedProductServiceServer) ListProducts(_ context.Context, _ *ListProductsRequest) (*ListProductsResponse, error) {
	return nil, errUnimplemented("ListProducts")
}
func (UnimplementedProductServiceServer) PlaceOrder(_ context.Context, _ *PlaceOrderRequest) (*PlaceOrderResponse, error) {
	return nil, errUnimplemented("PlaceOrder")
}

// RegisterProductServiceServer registers srv with the given gRPC server.
func RegisterProductServiceServer(s *grpc.Server, srv ProductServiceServer) {
	s.RegisterService(&ProductService_ServiceDesc, srv)
}

// ProductService_ServiceDesc is the service descriptor for ProductService.
// It mirrors what protoc-gen-go-grpc would generate.
var ProductService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: ServiceName,
	HandlerType: (*ProductServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "GetProduct",
			Handler:    _ProductService_GetProduct_Handler,
		},
		{
			MethodName: "ListProducts",
			Handler:    _ProductService_ListProducts_Handler,
		},
		{
			MethodName: "PlaceOrder",
			Handler:    _ProductService_PlaceOrder_Handler,
		},
	},
	Streams: []grpc.StreamDesc{},
}

func _ProductService_GetProduct_Handler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	req := &GetProductRequest{}
	if err := dec(req); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ProductServiceServer).GetProduct(ctx, req)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/" + ServiceName + "/GetProduct"}
	handler := func(ctx context.Context, req any) (any, error) {
		return srv.(ProductServiceServer).GetProduct(ctx, req.(*GetProductRequest))
	}
	return interceptor(ctx, req, info, handler)
}

func _ProductService_ListProducts_Handler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	req := &ListProductsRequest{}
	if err := dec(req); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ProductServiceServer).ListProducts(ctx, req)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/" + ServiceName + "/ListProducts"}
	handler := func(ctx context.Context, req any) (any, error) {
		return srv.(ProductServiceServer).ListProducts(ctx, req.(*ListProductsRequest))
	}
	return interceptor(ctx, req, info, handler)
}

func _ProductService_PlaceOrder_Handler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	req := &PlaceOrderRequest{}
	if err := dec(req); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ProductServiceServer).PlaceOrder(ctx, req)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/" + ServiceName + "/PlaceOrder"}
	handler := func(ctx context.Context, req any) (any, error) {
		return srv.(ProductServiceServer).PlaceOrder(ctx, req.(*PlaceOrderRequest))
	}
	return interceptor(ctx, req, info, handler)
}
