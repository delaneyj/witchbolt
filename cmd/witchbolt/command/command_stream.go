package command

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/delaneyj/witchbolt/stream"
)

type StreamCmd struct {
	Restore StreamRestoreCmd `cmd:"" help:"Restore a database from stream replicas"`
}

type StreamRestoreCmd struct {
	ConfigPath string `name:"config" short:"c" required:"" help:"Path to stream configuration file (YAML or JSON)" type:"path"`
	TargetPath string `name:"target" short:"t" help:"Override restore target path" type:"path"`
}

func (c *StreamRestoreCmd) Run() error {
	if c.ConfigPath == "" {
		return errors.New("config file is required")
	}
	cfg, err := loadStreamConfig(c.ConfigPath)
	if err != nil {
		return err
	}
	if c.TargetPath != "" {
		cfg.Restore.TargetPath = c.TargetPath
	}
	if cfg.Restore.TargetPath == "" {
		return errors.New("target path must be specified via config or --target")
	}
	return stream.RestoreStandalone(context.Background(), cfg)
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
