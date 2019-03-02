package cli

import (
	"github.com/pkg/errors"
)

// Copy the source to the target.
// Copy is only supported between a local directory and a dataset.
func (c *CLI) Copy(source, target string, tracker ProgressTracker) error {
	sourcePkg, sourcePath, err := splitPath(source)
	if err != nil {
		return err
	}

	targetPkg, targetPath, err := splitPath(target)
	if err != nil {
		return err
	}

	if sourcePkg == "" && targetPkg != "" {
		return c.Upload(sourcePath, c.client.Dataset(targetPkg), targetPath, tracker)
	}
	if sourcePkg != "" && targetPkg == "" && targetPath != "" {
		return c.Download(c.client.Dataset(sourcePkg), sourcePath, targetPath, tracker)
	}
	return errors.New("cp only supported between a dataset and a directory") // TODO
}
