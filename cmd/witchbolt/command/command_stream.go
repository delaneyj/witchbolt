package command

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"

	"github.com/delaneyj/witchbolt/stream"
)

func newStreamCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "stream",
		Short: "Stream replication helpers",
	}
	cmd.AddCommand(newStreamRestoreCommand())
	return cmd
}

type streamRestoreOptions struct {
	ConfigPath string
	TargetPath string
}

func newStreamRestoreCommand() *cobra.Command {
	var opts streamRestoreOptions
	cmd := &cobra.Command{
		Use:   "restore",
		Short: "Restore a database from stream replicas",
		RunE: func(cmd *cobra.Command, args []string) error {
			if opts.ConfigPath == "" {
				return errors.New("config file is required")
			}
			cfg, err := loadStreamConfig(opts.ConfigPath)
			if err != nil {
				return err
			}
			if opts.TargetPath != "" {
				cfg.Restore.TargetPath = opts.TargetPath
			}
			if cfg.Restore.TargetPath == "" {
				return errors.New("target path must be specified via config or --target")
			}
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}
			return stream.RestoreStandalone(ctx, cfg)
		},
	}
	cmd.Flags().StringVarP(&opts.ConfigPath, "config", "c", "", "Path to stream configuration file (YAML or JSON)")
	cmd.Flags().StringVarP(&opts.TargetPath, "target", "t", "", "Override restore target path")
	return cmd
}

func loadStreamConfig(path string) (stream.Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return stream.Config{}, err
	}
	var cfg stream.Config
	switch ext := strings.ToLower(filepath.Ext(path)); ext {
	case ".yaml", ".yml":
		if err := yaml.Unmarshal(data, &cfg); err != nil {
			return stream.Config{}, fmt.Errorf("parse yaml: %w", err)
		}
	case ".json":
		if err := json.Unmarshal(data, &cfg); err != nil {
			return stream.Config{}, fmt.Errorf("parse json: %w", err)
		}
	default:
		if err := yaml.Unmarshal(data, &cfg); err != nil {
			if err := json.Unmarshal(data, &cfg); err != nil {
				return stream.Config{}, fmt.Errorf("parse config: %w", err)
			}
		}
	}
	return cfg, nil
}
