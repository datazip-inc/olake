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
	"errors"
	"reflect"
	"strings"

	"github.com/go-playground/locales/en"
	ut "github.com/go-playground/universal-translator"
	"github.com/go-playground/validator/v10"
	en_translations "github.com/go-playground/validator/v10/translations/en"
)

// use a single instance, it caches struct info
var (
	uni      *ut.UniversalTranslator
	validate *validator.Validate
	trans    ut.Translator
)

func translateError(err error) (errs []string) {
	trans, _ := uni.GetTranslator("en")

	if err == nil {
		return nil
	}
	validatorErrs := err.(validator.ValidationErrors)
	for _, e := range validatorErrs {
		translatedErr := errors.New(e.Translate(trans))
		errs = append(errs, translatedErr.Error())
	}

	return errs
}

func Validate[T any](structure T) error {
	if err := validate.Struct(structure); err != nil {
		return errors.New(strings.Join(translateError(err), "; "))
	}

	return nil
}

func init() {
	// NOTE: omitting allot of error checking for brevity
	en := en.New()
	uni = ut.New(en, en)
	trans, _ = uni.GetTranslator("en")

	validate = validator.New(validator.WithRequiredStructEnabled())
	validate.RegisterTagNameFunc(func(fld reflect.StructField) string {
		name := strings.SplitN(fld.Tag.Get("json"), ",", 2)[0]
		if name == "" {
			name = strings.SplitN(fld.Tag.Get("yaml"), ",", 2)[0]
		}

		fieldName := fld.Name
		if name == "-" {
			return fieldName
		}

		if name != "" {
			return name
		}

		return fieldName
	})

	err := en_translations.RegisterDefaultTranslations(validate, trans)
	if err != nil {
		panic(err)
	}
}
