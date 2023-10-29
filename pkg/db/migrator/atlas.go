package migrator

import (
	"ariga.io/atlas-go-sdk/atlasexec"
	"context"
	"fmt"
	"go.uber.org/zap"
)

type Atlas struct {
	migrationDir    string
	atlasBinaryPath string
	atlasBinaryName string
}

type AtlasOptions struct {
	MigrationDir    string
	AtlasBinaryPath string
	AtlasBinaryName string
}

func NewAtlasMigrator(options *AtlasOptions) *Atlas {
	if options.AtlasBinaryName == "" {
		options.AtlasBinaryName = "atlas"
	}

	return &Atlas{
		migrationDir:    options.MigrationDir,
		atlasBinaryPath: options.AtlasBinaryPath,
		atlasBinaryName: options.AtlasBinaryName,
	}
}

func (a *Atlas) Migrate(ctx context.Context, dsn string) error {
	client, err := atlasexec.NewClient(a.atlasBinaryPath, a.atlasBinaryName)
	if err != nil {
		return err
	}

	logger := zap.L().Sugar()

	res, err := client.Apply(ctx, &atlasexec.ApplyParams{
		DirURL: a.migrationDir,
		URL:    dsn,
	})
	if err != nil {
		logger.Errorf("Failed to apply migrations: %s", err.Error())
		return err
	}

	logger.Infof("Current State: %s", res.Current)
	logger.Infof("Target State: %s", res.Target)

	for _, applied := range res.Applied {
		logger.Infof("Applied: %s", applied.Name)
	}

	if res.Error != "" {
		return fmt.Errorf("failed to apply: %s", res.Error)
	}

	return nil
}
