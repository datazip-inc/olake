package driver

import (
	"fmt"

	"github.com/datazip-inc/olake/types"
)

type Config struct {
	// Credentials for connecting to the Google Sheets API
	//
	// @jsonschema(
	// required=true,
	// oneOf=["Client","Service"]
	// )
	Credentials    interface{} `json:"credentials"`
	SpreadsheetID  string      `json:"spreadsheet_id" required:"true"`
	NameConversion *bool       `json:"name_conversion" required:"false" default:"true"`
}

// Authenticate via Google (OAuth)
type Client struct {
	// Enter your Google application Client ID
	//
	// @jsonschema(
	// title="Client ID"
	// )
	ClientID     string `json:"client_id" jsonschema:"title=Client ID,description=Enter your Google application's Client ID"`
	ClientSecret string `json:"client_secret" jsonschema:"title=Client Secret,description=Enter your Google application's Client Secret"`
	RefreshToken string `json:"refresh_token" title:"Refresh Token" required:"true" description:"Enter your Google application's refresh token"`
}

// @jsonschema=(title=Service Account Key Authentication)
type Service struct {
	ServiceAccountInfo string `json:"service_account_info" title:"Service Account Information." required:"true" description:"Enter your Google Cloud <a href='https://cloud.google.com/iam/docs/creating-managing-service-account-keys#creating_service_account_keys'>service account key</a> in JSON format" example:"'{ 'type': 'service_account', 'project_id': YOUR_PROJECT_ID, 'private_key_id': YOUR_PRIVATE_KEY, ... }'"`
}

func (c *Config) ValidateAndPopulateDefaults() error {
	if c.NameConversion == nil {
		c.NameConversion = types.ToPointer(true)
	}

	if c.Credentials == nil {
		return fmt.Errorf("credentials not found")
	}

	return nil
}
