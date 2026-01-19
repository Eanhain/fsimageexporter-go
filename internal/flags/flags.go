package flags

import (
	"fmt"

	"github.com/alecthomas/kong"
	"github.com/joho/godotenv"
)

type ServerFlags struct {
	fsimagePath string `short:"p" help:"Path to fsimage" default:"../tests/fsimage_2026-01-17_01-00" env:"FSIMAGE_PATH"`
}

func (sf *ServerFlags) Parse() {
	kong.Parse(sf)
}

func InitialFlags() (ServerFlags, error) {
	if err := godotenv.Load("../../.env"); err != nil {
		return ServerFlags{}, fmt.Errorf("couldn't import flags %w", err)
	}
	return ServerFlags{}, nil
}

func (sf ServerFlags) GetFSImagePath() string {
	return sf.fsimagePath
}
