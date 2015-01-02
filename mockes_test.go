package bubbles

import (
	"io"
	"net"
	"net/http"
	"testing"
)

type mockES struct {
	l net.Listener
}

// newMockES starts a fake bulk ES server.
func newMockES(tb testing.TB, fixture func() string) mockES {
	mux := http.NewServeMux()
	mux.HandleFunc("/_bulk", func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, fixture())
	})
	l, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		tb.Fatalf("listen error: %v", err)
	}
	go http.Serve(l, mux)
	return mockES{l: l}
}

func (m mockES) Stop() {
	m.l.Close()
}

func (m mockES) Addr() string {
	return m.l.Addr().String()
}
