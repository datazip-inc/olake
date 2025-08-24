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

package driver

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/testutils"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func ExecuteQuery(ctx context.Context, t *testing.T, streams []string, operation string, fileConfig bool) {
	t.Helper()

	var connStr string
	var config Config
	if fileConfig {
		utils.UnmarshalFile("./testdata/source.json", &config, false)
		connStr = fmt.Sprintf(
			"mongodb://%s:%s@%s/?authSource=%s&readPreference=%s",
			config.Username,
			config.Password,
			strings.Join(config.Hosts, ","),
			config.AuthDB,
			config.ReadPreference,
		)
	} else {
		connStr = "mongodb://localhost:27017"
	}
	db, ok := mongo.Connect(ctx, options.Client().ApplyURI(connStr))
	require.NoError(t, ok, "failed to connect to mongodb")

	switch operation {
	case "setup_cdc":
		// truncate the cdc tables
		for _, cdcStream := range streams {
			_, err := db.Database(config.Database).Collection(cdcStream).DeleteMany(ctx, bson.D{})
			require.NoError(t, err, fmt.Sprintf("failed to execute %s operation", operation), err)
		}
		return

	case "bulk_cdc_data_insert":
		backfillStreams := testutils.GetBackfillStreamsFromCDC(streams)
		totalRows := 15000000

		// TODO: insert data in batch
		// insert the data into the cdc tables concurrently
		err := utils.Concurrent(ctx, streams, len(streams), func(ctx context.Context, cdcStream string, executionNumber int) error {
			srcColl := db.Database(config.Database).Collection(backfillStreams[executionNumber-1])
			destColl := db.Database(config.Database).Collection(cdcStream)

			cursor, err := srcColl.Find(ctx, bson.D{}, options.Find().SetLimit(int64(totalRows)))
			if err != nil {
				return fmt.Errorf("stream: %s, error: %s", cdcStream, err)
			}
			defer cursor.Close(ctx)

			var docs []interface{}
			for cursor.Next(ctx) {
				var doc bson.M
				if err := cursor.Decode(&doc); err != nil {
					return err
				}
				docs = append(docs, doc)
			}
			if err := cursor.Err(); err != nil {
				return err
			}
			if len(docs) == 0 {
				return nil
			}
			_, err = destColl.InsertMany(ctx, docs)
			if err != nil {
				return fmt.Errorf("stream: %s, error: %s", cdcStream, err)
			}
			return nil
		})
		require.NoError(t, err, fmt.Sprintf("failed to execute %s operation", operation), err)
		return
	}
}
