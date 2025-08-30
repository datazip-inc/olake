/*
 * Copyright 2025 Olake By Datazip
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
