package cmd

import (
	"fmt"
	"os"
	"path"

	"github.com/mushanyux/MSIM/pkg/msutil"
	"github.com/spf13/cobra"
)

type stopCMD struct {
	ctx *MuShanIMContext
}

func newStopCMD(ctx *MuShanIMContext) *stopCMD {
	return &stopCMD{
		ctx: ctx,
	}
}

func (s *stopCMD) CMD() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "stop",
		Short: "stop the MuShanIM server",
		RunE:  s.run,
	}
	return cmd
}

func (s *stopCMD) run(cmd *cobra.Command, args []string) error {
	strb, _ := os.ReadFile(path.Join(".", pidfile))

	pid := msutil.ParseInt(string(strb))
	process, err := os.FindProcess(pid)
	if err != nil {
		return err
	}
	if process == nil {
		return nil
	}

	err = process.Kill()
	if err != nil {
		return err
	}
	fmt.Println("MuShanIM server stopped")
	return nil
}
