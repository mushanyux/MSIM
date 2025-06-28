package cmd

import (
	"github.com/spf13/cobra"
)

type MuShanIMContext struct {
}

type CMD interface {
	CMD() *cobra.Command
}
