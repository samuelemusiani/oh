package server

import (
	"crypto/sha256"
	"fmt"
	"log/slog"
	"os"

	oh "gitlab.lrz.de/protocol-design-ss26-meta-group-1-5/ass2_impl/go"
	"gitlab.lrz.de/protocol-design-ss26-meta-group-1-5/ass2_impl/go/internal"
	"gitlab.lrz.de/protocol-design-ss26-meta-group-1-5/ass2_impl/go/internal/chunker"
)

type Server struct {
	config        oh.Config
	multicastConn *internal.Multicast
	chunker       *chunker.Chunker

	files []*file

	l *slog.Logger
}

var (
	hashFunc = sha256.New
)

func New(c oh.Config) (*Server, error) {
	m, err := internal.OpenMulticastConnection(c.Address(), c.Port(), c.Interface())
	if err != nil {
		return nil, fmt.Errorf("error opening multicast connection: %w", err)
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey {
				return slog.Attr{} // drop it
			}
			return a
		},
		Level: slog.LevelDebug,
	}))

	return &Server{
		config:        c,
		multicastConn: m,
		chunker:       chunker.NewChunker(hashFunc()),
		l:             logger,
	}, nil
}

func (s *Server) SendFiles(files []string) error {
	var err error
	err = s.openFiles(files)
	if err != nil {
		return fmt.Errorf("error opening files: %w", err)
	}

	// Start server
	if err := s.run(); err != nil {
		return fmt.Errorf("error running server: %w", err)
	}

	return nil
}

func (s *Server) run() error {
	return fmt.Errorf("not implemented")
}
