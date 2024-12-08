package utils

import (
	"context"
	"fmt"

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

func ErrExecFormat(format string, function func() error) func() error {
	return func() error {
		if err := function(); err != nil {
			return fmt.Errorf(format, err)
		}

		return nil
	}
}
