package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/mengelbart/moqtransport"
	"github.com/mengelbart/moqtransport/webtransportmoq"
	"github.com/quic-go/webtransport-go"
)

const appName = "moqpub"

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	slog.SetDefault(logger)

	if err := run(os.Args); err != nil {
		slog.Error("error", "err", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet(appName, flag.ContinueOnError)

	addr := fs.String("addr", "https://localhost:4443/moq", "MOQ server URL (WebTransport)")
	name := fs.String("name", "test", "Stream name/namespace")
	insecure := fs.Bool("insecure", false, "Skip TLS verification")

	if err := fs.Parse(args[1:]); err != nil {
		return err
	}

	slog.Info("Connecting to MOQ server", "addr", *addr, "name", *name)

	// Connect via WebTransport
	tlsConfig := &tls.Config{
		InsecureSkipVerify: *insecure,
		NextProtos:         []string{"h3"},
	}

	dialer := webtransport.Dialer{
		TLSClientConfig: tlsConfig,
	}

	_, wtSession, err := dialer.Dial(context.Background(), *addr, nil)
	if err != nil {
		return fmt.Errorf("failed to dial WebTransport: %w", err)
	}
	defer wtSession.CloseWithError(0, "done")

	slog.Info("WebTransport connected")

	// Create MOQ session
	conn := webtransportmoq.NewClient(wtSession)
	session := &moqtransport.Session{
		Handler: moqtransport.HandlerFunc(func(w moqtransport.ResponseWriter, r *moqtransport.Message) {
			slog.Info("Received message", "method", r.Method)
		}),
		SubscribeHandler: moqtransport.SubscribeHandlerFunc(
			func(w *moqtransport.SubscribeResponseWriter, m *moqtransport.SubscribeMessage) {
				slog.Info("Received subscription request", "namespace", m.Namespace, "track", m.Track)
				// Accept subscription and start publishing
				if err := w.Accept(); err != nil {
					slog.Error("failed to accept subscription", "error", err)
					return
				}
				go publishFromStdin(w, m.Track)
			}),
		InitialMaxRequestID: 100,
	}

	if err := session.Run(conn); err != nil {
		return fmt.Errorf("failed to run session: %w", err)
	}

	slog.Info("MOQ session established")

	// Announce our namespace
	namespace := []string{*name}
	if err := session.Announce(context.Background(), namespace); err != nil {
		return fmt.Errorf("failed to announce namespace: %w", err)
	}

	slog.Info("Announced namespace", "namespace", namespace)
	slog.Info("Waiting for subscribers... (pipe fMP4 data to stdin)")

	// Wait forever (or until connection closes)
	select {}
}

func publishFromStdin(w moqtransport.Publisher, trackName string) {
	slog.Info("Starting to publish from stdin", "track", trackName)

	buf := make([]byte, 64*1024) // 64KB chunks
	groupID := uint64(0)
	objectID := uint64(0)

	for {
		n, err := os.Stdin.Read(buf)
		if err != nil {
			if err == io.EOF {
				slog.Info("EOF on stdin, stopping publish")
			} else {
				slog.Error("Error reading stdin", "error", err)
			}
			return
		}

		if n == 0 {
			continue
		}

		// Open subgroup and write object
		sg, err := w.OpenSubgroup(groupID, 0, 128)
		if err != nil {
			slog.Error("Failed to open subgroup", "error", err)
			return
		}

		_, err = sg.WriteObject(objectID, buf[:n])
		if err != nil {
			slog.Error("Failed to write object", "error", err)
			sg.Close()
			return
		}

		sg.Close()
		objectID++

		// New group every 30 objects (roughly 1 second at 30fps)
		if objectID%30 == 0 {
			groupID++
			objectID = 0
		}
	}
}
