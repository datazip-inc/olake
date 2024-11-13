package utils

import (
	"context"

	"github.com/hashicorp/go-multierror"
	"golang.org/x/sync/errgroup"
)

func ErrExec(functions ...func() error) error {
	group, _ := errgroup.WithContext(context.Background())
	for _, one := range functions {
		group.Go(one)
	}

	return group.Wait()
}

func ErrExecSequential(functions ...func() error) error {
	var multErr error
	for _, one := range functions {
		err := one()
		if err != nil {
			multErr = multierror.Append(multErr, err)
		}
	}

	return multErr
}
