package client

import (
	"github.com/docker/docker/api/types"
	"golang.org/x/net/context"
)

func (cli *Client) GetLayers(ctx context.Context, images []string) (map[string][]string, error) {
	return nil, nil
}