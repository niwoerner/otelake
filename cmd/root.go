package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/niwoerner/otelake/internal/config"
	"github.com/niwoerner/otelake/internal/server"
	"github.com/spf13/cobra"
)

const shutdownBudget = 10 * time.Second

var rootCmd = &cobra.Command{
	Use:   "otelake",
	Short: "otelake is a self-hosted OpenTelemetry backend.",
	RunE: func(cmd *cobra.Command, args []string) error {
		logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
		logger.Info("Starting otelake",
			"version", cmd.Version,
			"commit", cmd.Annotations["commit"],
			"date", cmd.Annotations["date"],
		)

		ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
		defer stop()

		srv := server.New(config.NewConfig(), logger)

		defer func() {
			sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownBudget)
			defer cancel()

			if err := srv.Shutdown(sctx); err != nil {
				logger.Error("Error shutting down otelake", "error", err)
			}
		}()

		if err := srv.Run(ctx); err != nil {
			return fmt.Errorf("running otelake: %w", err)
		}

		<-ctx.Done()
		logger.Info("Stopping otelake")
		return nil
	},
}

// Execute runs the root command. It is called by main.main().
func Execute(version, commit, date string) {
	rootCmd.Version = version
	rootCmd.Annotations = map[string]string{
		"commit": commit,
		"date":   date,
	}

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
