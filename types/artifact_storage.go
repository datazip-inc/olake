package types

type ArtifactStorageConfig struct {
	Bucket         string `json:"bucket"`
	Region         string `json:"region"`
	BasePath       string `json:"base_path"`
	AccessKey      string `json:"access_key,omitempty"`
	SecretKey      string `json:"secret_key,omitempty"`
	SessionToken   string `json:"session_token,omitempty"`
	Endpoint       string `json:"endpoint,omitempty"`
	UseSSL         bool   `json:"use_ssl,omitempty"`
	PathStyle      bool   `json:"path_style,omitempty"`
	UploadInterval string `json:"upload_interval,omitempty"`
}
