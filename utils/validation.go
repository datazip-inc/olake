package utils

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/utils/errs"
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

const (
	codeMaxThreadsInvalid = "config.max_threads_invalid"
	codeRetryCountInvalid = "config.retry_count_invalid"
)

// applyNonNegativeDefault rejects a negative value and applies fallback when the field is unset (0).
func applyNonNegativeDefault(value *int, fallback int, name, code string) error {
	if *value < 0 {
		return errs.Precondition(errs.ConfigInvalid, code,
			fmt.Errorf("%s is invalid: %d, must be non-negative", name, *value))
	}
	if *value == 0 {
		*value = fallback
	}
	return nil
}

// ApplyMaxThreadsDefault rejects a negative max-threads value and applies
// constants.DefaultThreadCount when the field is unset (0).
func ApplyMaxThreadsDefault(maxThreads *int) error {
	return applyNonNegativeDefault(maxThreads, constants.DefaultThreadCount, "max threads", codeMaxThreadsInvalid)
}

// ApplyRetryCountDefault rejects a negative retry-count value and applies
// constants.DefaultRetryCount when the field is unset (0).
func ApplyRetryCountDefault(retryCount *int) error {
	return applyNonNegativeDefault(retryCount, constants.DefaultRetryCount, "retry count", codeRetryCountInvalid)
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
