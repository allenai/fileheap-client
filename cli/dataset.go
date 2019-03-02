package cli

import (
	"fmt"
	"strings"
)

// MakeDataset creates a new dataset.
func (c *CLI) MakeDataset() error {
	dataset, err := c.client.NewDataset(c.ctx)
	if err != nil {
		return err
	}

	fmt.Printf("fh://%s\n", dataset.Name())
	return nil
}

// SealDataset seals a dataset against future modification.
func (c *CLI) SealDataset(name string) error {
	name = strings.TrimPrefix(name, "fh://")
	if err := c.client.Dataset(name).Seal(c.ctx); err != nil {
		return err
	}

	fmt.Printf("Sealed fh://%s\n", name)
	return nil
}
