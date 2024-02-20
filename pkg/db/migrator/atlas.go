package migrator

import (
	"ariga.io/atlas-go-sdk/atlasexec"
	"context"
	"fmt"
	"github.com/vectrum-io/strongforce/pkg/db"
)

type Atlas struct {
	migrationDir    string
	atlasBinaryPath string
	atlasBinaryName string
	baseline        string
}

type AtlasOptions struct {
	MigrationDir    string
	AtlasBinaryPath string
	AtlasBinaryName string
	BaselineVersion string
}

func NewAtlasMigrator(options *AtlasOptions) *Atlas {
	if options.AtlasBinaryName == "" {
		options.AtlasBinaryName = "atlas"
	}

	return &Atlas{
		migrationDir:    options.MigrationDir,
		atlasBinaryPath: options.AtlasBinaryPath,
		atlasBinaryName: options.AtlasBinaryName,
		baseline:        options.BaselineVersion,
	}
}

func (a *Atlas) Migrate(ctx context.Context, dsn string) (*db.MigrationResult, error) {
	client, err := atlasexec.NewClient(a.atlasBinaryPath, a.atlasBinaryName)
	if err != nil {
		return nil, fmt.Errorf("failed to create atlas client: %w", err)
	}

	res, err := client.MigrateApply(ctx, &atlasexec.MigrateApplyParams{
		URL:             dsn,
		DirURL:          fmt.Sprintf("file://%s", a.migrationDir),
		BaselineVersion: a.baseline,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to apply migrations: %w", err)
	}

	if res.Error != "" {
		return nil, fmt.Errorf("failed to apply: %s", res.Error)
	}

	appliedMigrations := make([]string, len(res.Applied))

	for i, applied := range res.Applied {
		appliedMigrations[i] = applied.File.Name
	}

	return &db.MigrationResult{AppliedMigrations: appliedMigrations}, nil
}
