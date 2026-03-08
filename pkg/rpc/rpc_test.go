package rpc_test

import (
	"context"
	"log/slog"
	"net"
	"testing"
	"time"

	productv1 "github.com/miladhzz/gkit/api/product/v1"
	"github.com/miladhzz/gkit/pkg/rpc"
	"github.com/miladhzz/gkit/pkg/rpc/codec"
	"github.com/miladhzz/gkit/pkg/rpc/interceptors"
	"google.golang.org/grpc"
)

func init() {
	codec.Register()
}

// testProductServer is a minimal ProductService implementation for tests.
type testProductServer struct {
	productv1.UnimplementedProductServiceServer
}

func (s *testProductServer) GetProduct(_ context.Context, req *productv1.GetProductRequest) (*productv1.Product, error) {
	return &productv1.Product{ID: req.ID, Name: "Test Product", Price: 9.99}, nil
}

func (s *testProductServer) ListProducts(_ context.Context, _ *productv1.ListProductsRequest) (*productv1.ListProductsResponse, error) {
	return &productv1.ListProductsResponse{
		Products: []*productv1.Product{
			{ID: "1", Name: "A", Price: 1.0},
		},
	}, nil
}

func (s *testProductServer) PlaceOrder(_ context.Context, req *productv1.PlaceOrderRequest) (*productv1.PlaceOrderResponse, error) {
	return &productv1.PlaceOrderResponse{OrderID: "ord-1", Status: "accepted"}, nil
}

// startTestServer starts a gRPC server on a random port and returns the address.
func startTestServer(t *testing.T) string {
	t.Helper()

	logger := slog.Default()
	srv := rpc.NewServer(
		rpc.WithUnaryInterceptors(
			interceptors.Recovery(logger),
			interceptors.Logging(logger),
		),
	)
	productv1.RegisterProductServiceServer(srv.Server(), &testProductServer{})

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	go func() {
		if err := srv.Server().Serve(lis); err != nil && err != grpc.ErrServerStopped {
			t.Logf("server error: %v", err)
		}
	}()

	t.Cleanup(srv.GracefulStop)
	return lis.Addr().String()
}

func TestRPC_GetProduct(t *testing.T) {
	addr := startTestServer(t)

	conn, err := rpc.Dial(addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	client := productv1.NewProductServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	resp, err := client.GetProduct(ctx, &productv1.GetProductRequest{ID: "p1"})
	if err != nil {
		t.Fatalf("GetProduct: %v", err)
	}
	if resp.ID != "p1" {
		t.Errorf("expected ID 'p1', got %q", resp.ID)
	}
}

func TestRPC_ListProducts(t *testing.T) {
	addr := startTestServer(t)

	conn, err := rpc.Dial(addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	client := productv1.NewProductServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	resp, err := client.ListProducts(ctx, &productv1.ListProductsRequest{PageSize: 10})
	if err != nil {
		t.Fatalf("ListProducts: %v", err)
	}
	if len(resp.Products) == 0 {
		t.Error("expected at least one product")
	}
}

func TestRPC_PlaceOrder(t *testing.T) {
	addr := startTestServer(t)

	conn, err := rpc.Dial(addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	client := productv1.NewProductServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	resp, err := client.PlaceOrder(ctx, &productv1.PlaceOrderRequest{
		ProductID: "p1",
		Quantity:  2,
	})
	if err != nil {
		t.Fatalf("PlaceOrder: %v", err)
	}
	if resp.Status != "accepted" {
		t.Errorf("expected status 'accepted', got %q", resp.Status)
	}
}
