package client

import (
	"github.com/docker/docker/api/types"
	"golang.org/x/net/context"
)

func (cli *Client) AllRootFS(ctx context.Context) (map[string]types.RootFS, error) {
	return nil, nil
}