package driver

import (
	"net/url"
	"strings"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
)

type Config struct {
	Hosts            []string `json:"hosts"`
	Database         string   `json:"database"`
	AuthDB           string   `json:"authdb"`
	Username         string   `json:"username"`
	Password         string   `json:"password"`
	ReplicaSet       string   `json:"replica_set"`
	ReadPreference   string   `json:"read_preference"`
	Srv              bool     `json:"srv"`
	MaxThreads       int      `json:"max_threads"`
	RetryCount       int      `json:"backoff_retry_count"`
	ChunkingStrategy string   `json:"chunking_strategy"`
	ServerRAM        int      `json:"server_ram"`
}

func (c *Config) URI() string {
	connectionPrefix := "mongodb"
	if c.Srv {
		connectionPrefix = "mongodb+srv"
	}

	if c.MaxThreads == 0 {
		// set default threads
		logger.Info("setting max threads to default[%d]", constants.DefaultThreadCount)
		c.MaxThreads = constants.DefaultThreadCount
	}

	// Build query parameters
	query := url.Values{}
	query.Set("authSource", c.AuthDB)
	if c.ReplicaSet != "" {
		query.Set("replicaSet", c.ReplicaSet)
		if c.ReadPreference == "" {
			c.ReadPreference = constants.DefaultReadPreference
		}
		query.Set("readPreference", c.ReadPreference)
	}

	host := strings.Join(c.Hosts, ",")

	// Construct final URI using url.URL
	u := &url.URL{
		Scheme:   connectionPrefix,
		User:     utils.Ternary(c.Password != "", url.UserPassword(c.Username, c.Password), url.User(c.Username)).(*url.Userinfo),
		Host:     host,
		Path:     "/",
		RawQuery: query.Encode(),
	}

	return u.String()
}

func (c *Config) Validate() error {
	return utils.Validate(c)
}
