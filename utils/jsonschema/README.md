[![Build Status](https://travis-ci.org/brainicorn/jsonschemagen.svg?branch=master)](https://travis-ci.org/brainicorn/jsonschemagen)
[![codecov](https://codecov.io/gh/brainicorn/jsonschemagen/branch/master/graph/badge.svg)](https://codecov.io/gh/brainicorn/jsonschemagen)
[![Go Report Card](https://goreportcard.com/badge/github.com/datazip-inc/olake/jsonschema)](https://goreportcard.com/report/github.com/datazip-inc/olake/jsonschema)
[![GoDoc](https://godoc.org/github.com/datazip-inc/olake/jsonschema?status.svg)](https://godoc.org/github.com/datazip-inc/olake/jsonschema)

# jsonschemagen

JSON SchemaGen is a library and command-line tool that allows generating a JSON-Schema by annotating (java-style) the same GO structs used for your JSON representations.

Essentially, the same structs you use to consume JSON can also be used to generate a JSON-Schema.

**API Documentation:** [https://godoc.org/github.com/datazip-inc/olake/jsonschema](https://godoc.org/github.com/datazip-inc/olake/jsonschema)

[Issue Tracker](https://github.com/datazip-inc/olake/jsonschema/issues)

**Annotation Guide:** [jsonSchema annotation guide](annotation_guide.md)

## Installation

The most common way to use jsonschemagen is as a command-line tool executed via `go generate`. To install the CLI tool, simply run:

```bash
> go get -v github.com/datazip-inc/olake/jsonschema
```

Once installed, you can add a comment in your main.go (or some other go file in your root package) to integrate jsonschemagen with the go generate tool.
Something along the lines of:

```go
//go:generate jsonschemagen <options> <root package> <root type>
```

With that in place, you can generate your schema by simply running

```bash
> go generate
```

in the root of your project.

_options, root package and root type are explained below_

## Usage

### Basic command-line help

#### Synopsis

jsonschemagen is a commandline tool for generating json-schema from Go code.

jsonschemagen takes a base package and a root object
and generates a full json-schema for all involved types.

It will generate very basic schema without any code changes,
however, java-style annotations can be used to customize and
generate complex schemas that follow the json-schema spec.

For more information, see http://json-schema.org/

```
jsonschemagen [base package] [root type]
```

#### Options

```
  -c, --codegen            generate go code to access schemas as strings
  -d, --debug              enable debug logging
  -f, --filename string    filename for root schema (default is calculated using pkg and type)
  -t, --include-tests      load test files when parsing
  -i, --inline-def         use inline schemas rather than json-refs
  -o, --output string      output directory for files (default is ./schema) (default "./schema")
  -q, --quiet              disable all logging
  -r, --remove-dir         removes the output dir and all of it's files before generation
  -s, --separate-files     generate separate files for each definition
  -x, --suppress-x-attrs   supress non-standard attributes
  -v, --verbose            enable verbose logging
```

### In-depth

In it's simplest form the command-line tool takes a base package and a root type.
Since a schema has to have a root, we've opted to pass in the package and type explicitly to avoid having to search the entire source tree for the starting point.
This may seem like extra typeutils, but it saves a ton of time in terms of performance of the tool.

**Base Package:** This is the package where the root type can be found. This needs to be the full package declaration as it appears in an import statement.

example: github.com/exampletstore

**Root Type:** This is the GO struct that acts as the root of the schema. This must be located within the base package.

example: Store

Putting it all together we get:

```go
> jsonschemagen github.com/exampletstore Store
```

#### Options In-Depth

| Option                  | Description                                                                                                                                                                                                                                                                                         |
| ----------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `-c, --codegen`         | This option will generate a file named _schema_accessor.go_ which contains the main schema and any/all definition schemas as string constants. This is useful for doing validation within GO code without having to use io to load the schema                                                       |
| `-f, --filename string` | The filename for the root schema. By default it will be calculated using the import path and type of the root object. This option let's you name it something predictable like "schema.json"                                                                                                        |
| `-o, --output string`   | The output directory for files. Can be absolute or relative to where the command was run. Defaults to ./schema When using with _go generate_ it's important to put the go:generate comment in a file that's in the root of your project so relative output paths are relative to your project root. |
| `-t, --include-tests`   | This will tell the code parser to load/consider test files. This is usually not needed and adds a lot of time to the code parsing operations.                                                                                                                                                       |
| `-i, --inline-def`      | If this flag is passed, all definitions will be included as full inline schemas rather than using $ref with a definitions node. This is usually not passed in favor of reusing definitions with $refs.                                                                                              |
| `-r, --remove-dir`      | When this flag is passed the tool will remove the output folder and all files within it before generation. This ensures old schemas are removed, however, be careful not to use an actual go package with source code as your output with this option or your go code will also be deleted.         |
| `-s, --separate-files`  | When this option is passed, the complete root schema will be generated as well as individual files for each encountered definition. Each definition file is complete (with it's own definitions) and can be used standalone to validate a subset of the complete root schema.                       |
| `-d, --debug`           | Turns on debug logging. You can turn it on but the output is very ugly at this point                                                                                                                                                                                                                |
| `-v, --verbose`         | Turns on verbose logging which is even more non-sensical than debug logging.                                                                                                                                                                                                                        |
| `-q, --quiet`           | Turns off all log output entirely                                                                                                                                                                                                                                                                   |

**Example:** With the following go:generate comment in our main.go, we'll generate a root schema, separate definition schemas, and a go file with schema contants in a folder named "petschema" which is deleted before each run.

```go
//go:generate jsonschemagen -s -c -r -o ./petschema github.com/exampletstore Store
```

### Annotations

Although the jsonschemgen tool will generate completely valid shemas with zero code changes whatsoever, it also supports using ["java-style" annotations](https://github.com/brainicorn/ganno) within code comments to enhance the resulting schema with directives found in the [json-schema spec](http://json-schema.org/). These include (but are not limited) to things like required fields, mix/max lengths, regex patterns, etc, etc.

Here's a quick example showing a very limited set of things that are possible:

```go
// Store is the base type for a pet store example schema.
type Store struct {
	// ID is the uuid for the store.
	//
	// @jsonSchema(required=true, format="uuid")
	ID string `json:"id,omitempty"`

	// Type is the type of pet store.
	// The can be "common" or "exotic"
	//
	// @jsonSchema(required=true, pattern="^common$|^exotic$")
	Type string `json:"type,omitempty"`

	// Name is the name of the store.
	//
	// @jsonSchema(required=true, maxLength=250)
	Name string `json:"name,omitempty"`

	// Location is a custom interface that defines multiple types of locations.
	// See the interface definition below for how this is handled when generating the schema
	Location Location `json:"location,omitempty"`

	// cacheKey is a private member and will not be included in the schema
	cacheKey string
}

// Location is an interface for a location which can be of multiple types.
// The "oneOf" attribute below takes fully-qualified package/type paths to other go objects.
// These can be in any package and can also be located in 3rd party libraries.
//
// @jsonSchema(
// 	oneOf=["github.com/example/petstore/Address"
// 		,"github.com/example/petstore/MapCoordinates"
// 	]
// )
type Location interface {}

```

For in-depth details about using the @jsonSchema annotations, please [see our jsonSchema annotation guide](annotation_guide.md)

### Validation

The jsonschemagen tool can only be used to generate schemas and as such does not include any tools/code to validate input against the generated schema.

For validation, we've been using the excellent [gojsonschema library](https://github.com/xeipuuv/gojsonschema)

If you combine the gojsonschema library with this library's ability to generate schemas as GO string constants, validation becomes trivial:

```go
import (
	"github.com/xeipuuv/gojsonschema"
	"github.com/example/petstore/petschema"
)

func main() {
	someInput := `some json blob goes here`

	inputLoader := gojsonschema.NewStringLoader(someInput)

	schemaLoader := gojsonschema.NewStringLoader(petschema.GithubComExamplePetstoreStore)

	result, err := gojsonschema.Validate(schemaLoader, inputLoader)

	if err != nil {
        panic(err.Error())
    }

    if result.Valid() {
        fmt.Printf("The document is valid\n")
    } else {
        fmt.Printf("The document is not valid. see errors :\n")
        for _, desc := range result.Errors() {
            fmt.Printf("- %s\n", desc)
        }
    }
}
```

**API Documentation:** [https://godoc.org/github.com/datazip-inc/olake/jsonschema](https://godoc.org/github.com/datazip-inc/olake/jsonschema)

[Issue Tracker](https://github.com/datazip-inc/olake/jsonschema/issues)

## Contributors

Pull requests, issues and comments welcome. For pull requests:

- Add tests for new features and bug fixes
- Follow the existing style
- Separate unrelated changes into multiple pull requests

See the existing issues for things to start contributing.

For bigger changes, make sure you start a discussion first by creating
an issue and explaining the intended change.

## License

Apache 2.0 licensed, see [LICENSE](LICENSE) file.
