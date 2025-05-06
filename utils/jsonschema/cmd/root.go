package protocol

import (
	"bufio"
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"text/template"
	"time"
	"unicode"

	"github.com/goccy/go-json"

	"github.com/datazip-inc/olake/jsonschema/generator"
	"github.com/datazip-inc/olake/jsonschema/schema"

	"github.com/spf13/cobra"
)

const (
	goFileName = "schema_accessor.go"
)

type templateData struct {
	VarName string
	Schema  string
}

// RootCmd is the main jsonschemagen command.
type RootCmd struct {
	Cmd            *cobra.Command
	opts           generator.Options
	includeTests   bool
	inlineDefs     bool
	logQuiet       bool
	logDebug       bool
	logVerbose     bool
	codegen        bool
	defFiles       bool
	removeDir      bool
	outputDir      string
	rootFilename   string
	basePackage    string
	rootType       string
	gen            *generator.JSONSchemaGenerator
	suppressXAttrs bool
}

// NewRootCommand creates a new instance of the RootCmd.
func NewRootCommand() *RootCmd {
	rc := &RootCmd{}
	rc.Cmd = &cobra.Command{
		Use:   "jsonschemagen [base package] [root type]",
		Short: "A commandline tool for generating json-schema from Go code",
		Long: `jsonschemagen is a commandline tool for generating json-schema from Go code.

jsonschemagen takes a base package and a root object
and generates a full json-schema for all involved types.

It will generate very basic schema without any code changes,
however, java-style annotations can be used to customize and
generate complex schemas that follow the json-schema spec.

For more information, see http://json-schema.org/`,
		RunE: rc.doGeneration,
	}

	flags := rc.Cmd.Flags()
	flags.BoolVarP(&rc.includeTests, "include-tests", "t", false, "load test files when parsing")
	flags.BoolVarP(&rc.inlineDefs, "inline-def", "i", false, "use inline schemas rather than json-refs")
	flags.BoolVarP(&rc.logQuiet, "quiet", "q", false, "disable all logging")
	flags.BoolVarP(&rc.logDebug, "debug", "d", false, "enable debug logging")
	flags.BoolVarP(&rc.logVerbose, "verbose", "v", false, "enable verbose logging")
	flags.BoolVarP(&rc.codegen, "codegen", "c", false, "generate go code to access schemas as strings")
	flags.BoolVarP(&rc.removeDir, "remove-dir", "r", false, "removes the output dir and all of it's files before generation")
	flags.BoolVarP(&rc.defFiles, "separate-files", "s", false, "generate separate files for each definition")
	flags.StringVarP(&rc.outputDir, "output", "o", "./schema", "output directory for files (default is ./schema)")
	flags.StringVarP(&rc.rootFilename, "filename", "f", "", "filename for root schema (default is calculated using pkg and type)")
	flags.BoolVarP(&rc.suppressXAttrs, "suppress-x-attrs", "x", false, "supress non-standard attributes")
	return rc
}

// Execute runs the main command.
// This is called by main.main(). It only needs to happen once to the RootCmd.
func (c *RootCmd) Execute() {
	if err := c.Cmd.Execute(); err != nil {
		log.Fatal(err)
	}
}

func (c *RootCmd) getLogLevel() generator.LogLevel {
	if c.logQuiet {
		return generator.QuietLevel
	}

	if c.logDebug && !c.logVerbose {
		return generator.DebugLevel
	}

	if c.logVerbose {
		return generator.VerboseLevel
	}

	return generator.InfoLevel
}

func (c *RootCmd) doGeneration(cmd *cobra.Command, args []string) error {
	var err error
	var rootSchema schema.JSONSchema

	start := time.Now()

	if len(args) < 2 {
		cmd.Usage()
		os.Exit(0)
	}

	if !c.isPackageString(args[0]) {
		return fmt.Errorf("invalid package specifier %s", args[0])
	}

	if !c.isIdent(args[1]) {
		return fmt.Errorf("invalid type specifier %s", args[1])
	}

	c.basePackage = args[0]
	c.rootType = args[1]
	opts := generator.NewOptions()
	opts.LogLevel = c.getLogLevel()
	opts.AutoCreateDefs = !c.inlineDefs
	opts.IncludeTests = c.includeTests
	opts.SupressXAttrs = c.suppressXAttrs
	c.opts = opts
	c.gen = generator.NewJSONSchemaGenerator(c.basePackage, c.rootType, opts)

	rootSchema, err = c.gen.Generate()

	if err == nil {
		err = c.writeSchemaFiles(rootSchema)
	}

	c.gen.LogInfo("total generation took ", time.Since(start))
	return err
}

func (c *RootCmd) writeSchemaFiles(rootSchema schema.JSONSchema) error {
	var err error
	var absOutputDir string
	var schemaBytes []byte
	var codeBuffer bytes.Buffer
	var codeFile *os.File
	var defSchema schema.JSONSchema
	var gofname string

	tmpl, _ := template.New("schemaTemplate").Parse("\t// {{.VarName}} is a json-schema accessor\n\t{{.VarName}} = `{{.Schema}}`\n\n")

	absOutputDir, err = filepath.Abs(c.outputDir)
	c.gen.LogInfo("remove dir? ", c.removeDir)
	if err == nil {
		if c.removeDir {
			c.gen.LogInfo("removing dir ", absOutputDir)
			err = os.RemoveAll(absOutputDir)
			c.gen.LogVerbose("remove err is ", err)
		}

		if err == nil {
			err = os.MkdirAll(absOutputDir, os.ModePerm)
		}
	}

	if err == nil {
		schemaBytes, err = json.MarshalIndent(rootSchema, "", "  ")
	}

	if err == nil {
		//write the main schema file
		fname := refToFilename(c.basePackage + "/" + c.rootType)
		if len(strings.TrimSpace(c.rootFilename)) > 0 {
			fname = c.rootFilename
		}

		schemaPath := filepath.Join(absOutputDir, fname)

		err = ioutil.WriteFile(schemaPath, schemaBytes, 0664)

		c.gen.LogInfo("wrote file ", schemaPath)
	}

	if c.codegen {

		if err == nil {
			goPkg := packageFromOutputDir(absOutputDir)
			codeBuffer.WriteString("package " + goPkg + "\n\nconst (\n")
		}

		// check to see if the accessor already exists and load it into buffer if it does
		gofname = filepath.Join(absOutputDir, goFileName)
		codeFile, err = os.Open(gofname)

		if err == nil {
			scanner := bufio.NewScanner(codeFile)
			foundConst := false
			for scanner.Scan() {
				line := scanner.Text()
				if !foundConst && line == "const (" {
					foundConst = true
					continue
				} else if foundConst {
					if line == ")" {
						break
					} else {
						codeBuffer.WriteString(line + "\n")
					}
				}
			}
			codeFile.Close()
			codeFile, err = os.OpenFile(gofname, os.O_RDWR, 0666)
		} else {
			codeFile, err = os.Create(gofname)
		}

		if err == nil {
			schemaBytes, err = json.Marshal(rootSchema)
			if err == nil {
				err = tmpl.Execute(&codeBuffer, templateData{VarName: refToVarName(c.basePackage + "/" + c.rootType), Schema: string(schemaBytes)})
			}
		}
	}

	if err == nil && (c.defFiles || c.codegen) {
		for defK := range rootSchema.GetDefinitions() {
			p, t := refToPackageAndType(defK)
			defSchema, err = c.gen.SubGenerate(p, t)

			if c.defFiles {
				schemaBytes, err = json.MarshalIndent(defSchema, "", "  ")

				if err == nil {
					schemaPath := filepath.Join(absOutputDir, refToFilename(defK))
					err = ioutil.WriteFile(schemaPath, schemaBytes, 0664)
					c.gen.LogInfo("wrote file ", schemaPath)
				}
			}

			if c.codegen {
				if err == nil {
					schemaBytes, err = json.Marshal(defSchema)
					if err == nil {
						err = tmpl.Execute(&codeBuffer, templateData{VarName: refToVarName(defK), Schema: string(schemaBytes)})
					}
				}
			}
		}
	}

	if err == nil && c.codegen {
		_, err = codeBuffer.WriteString(")\n")

		if err == nil {
			_, err = codeFile.WriteString(codeBuffer.String())
			c.gen.LogInfo("wrote file ", gofname)
		}
	}

	return err
}

func (c *RootCmd) isIdent(name string) bool {
	ident := true

	for _, r := range name {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '_' {
			ident = false
			break
		}
	}

	return ident
}

func (c *RootCmd) isPackageString(name string) bool {
	if strings.HasPrefix(name, "/") {
		return false
	}

	if strings.HasSuffix(name, "/") {
		return false
	}

	return true
}

func refToPackageAndType(ref string) (string, string) {

	s := ref
	s = strings.Replace(s, "_", ".", -1)
	s = strings.Replace(s, "-", "/", -1)

	pkg := s[:strings.LastIndex(s, "/")]
	typ := s[strings.LastIndex(s, "/")+1:]

	return pkg, typ

}

func refToFilename(ref string) string {
	s := ref
	s = strings.Replace(s, "#/definitions/", "", -1)
	s = strings.Replace(s, ".", "_", -1)
	s = strings.Replace(s, "/", "-", -1)

	s = s + ".json"

	return s

}

func refToVarName(ref string) string {
	s := ref
	s = strings.Replace(s, "#/definitions/", "", -1)
	s = strings.Replace(s, "_", " ", -1)
	s = strings.Replace(s, "-", " ", -1)
	s = strings.Replace(s, ".", " ", -1)
	s = strings.Replace(s, "/", " ", -1)

	s = strings.Title(s)

	s = strings.Map(func(r rune) rune {
		if unicode.IsSpace(r) {
			return -1
		}
		return r
	}, s)

	return s

}

func packageFromOutputDir(dir string) string {
	path := strings.TrimSuffix(dir, "/")
	return path[strings.LastIndex(path, "/")+1:]
}
