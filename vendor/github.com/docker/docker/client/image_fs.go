package client

import (
	"golang.org/x/net/context"
)

func (cli *Client) GetLayers(ctx context.Context, authConfig string) ([]string, error) {
	return nil, nil
}

func (cli *Client) QueryLayersByImage(ctx context.Context, image, encodedAuth string) ([]string, error) {
	return nil, nil
}