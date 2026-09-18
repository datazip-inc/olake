package utils

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/datazip-inc/olake/constants"
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

// ApplyMaxThreadsDefault rejects a negative max-threads value and applies
// constants.DefaultThreadCount when the field is unset (0).
func ApplyMaxThreadsDefault(maxThreads *int) error {
	if *maxThreads < 0 {
		return fmt.Errorf("max threads is invalid")
	}
	if *maxThreads == 0 {
		*maxThreads = constants.DefaultThreadCount
	}
	return nil
}

// ApplyRetryCountDefault rejects a negative retry-count value and applies
// constants.DefaultRetryCount when the field is unset (0).
func ApplyRetryCountDefault(retryCount *int) error {
	if *retryCount < 0 {
		return fmt.Errorf("retry count is invalid")
	}
	if *retryCount == 0 {
		*retryCount = constants.DefaultRetryCount
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
