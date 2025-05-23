package generator

import (
	"fmt"
	"go/ast"
	"reflect"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"golang.org/x/tools/go/loader"
)

var builtinTypes = map[string]string{
	"string":    "string",
	"bool":      "boolean",
	"float32":   "number",
	"float64":   "number",
	"int":       "integer",
	"int8":      "integer",
	"int16":     "integer",
	"int32":     "integer",
	"int64":     "integer",
	"uint":      "integer",
	"uint8":     "integer",
	"uint16":    "integer",
	"uint32":    "integer",
	"uint64":    "integer",
	"time.Time": "string",
	"net.IP":    "string",
	"url.URL":   "string",
	"[]byte":    "string",
}

var jsonTypes = map[string][]string{
	"string":  {"string", "time.Time", "net.IP", "url.URL", "[]byte"},
	"boolean": {"bool"},
	"number":  {"float32", "float64"},
	"integer": {"int", "int8", "int16", "int32", "int64", "uint", "uint8", "uint16", "uint32", "uint64"},
	"array":   {},
}

func (g *JSONSchemaGenerator) findDeclInfoForPackage(pkg *loader.PackageInfo, file *ast.File, typeToFind string) (*declInfo, error) {
	g.LogDebug("looking for Decl Info...")
	var dInfo *declInfo

	// first check the local file
	if file != nil {
		dInfo = g.findDeclInFile(pkg, file, typeToFind)
	}

	if dInfo != nil {
		g.LogVerbose("found decl in current file")
		return dInfo, nil
	}

	// didn't find it in the current file, look through the rest of the current Package files
	for _, packageFile := range pkg.Files {
		if packageFile == file {
			continue
		}

		dInfo = g.findDeclInFile(pkg, packageFile, typeToFind)

		if dInfo != nil {
			g.LogVerbose("found decl in package file ", packageFile.Name.Name)
			return dInfo, nil
		}
	}

	return nil, fmt.Errorf("could not find type '%s' in package %s", typeToFind, pkg.Pkg.Path())
}

func (g *JSONSchemaGenerator) findDeclInfoForSelector(ownerDecl *declInfo, selector *ast.SelectorExpr) (*declInfo, error) {

	return g.findDeclInfoForPackage(g.program.AllPackages[ownerDecl.pkg.Uses[selector.Sel].Pkg()], nil, selector.Sel.Name)
}

func (g *JSONSchemaGenerator) findDeclInFile(pkgInfo *loader.PackageInfo, gofile *ast.File, typeToFind string) *declInfo {
	for _, decl := range gofile.Decls {
		gd, ok := decl.(*ast.GenDecl)
		if !ok {
			continue
		}
		for _, spc := range gd.Specs {
			if ts, ok := spc.(*ast.TypeSpec); ok {
				if ts.Name.Name == typeToFind {
					return g.newDeclInfo(pkgInfo, gofile, gd, ts)
				}

			}
		}
	}

	return nil
}

func isJSONType(name string) bool {
	_, ok := jsonTypes[name]

	return ok
}

func isSelfRef(name string) bool {
	return name == "#"
}

func isIdent(name string) bool {
	ident := true

	for _, r := range name {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '_' {
			ident = false
			break
		}
	}

	return ident
}

func isPackageType(name string) bool {
	if strings.HasPrefix(name, "/") {
		return false
	}

	if !strings.Contains(name, "/") {
		return false
	}

	chunks := strings.SplitAfter(name, "/")
	typeName := chunks[len(chunks)-1]
	firstTypeRune, _ := utf8.DecodeRuneInString(typeName)

	if !isIdent(typeName) || !unicode.IsUpper(firstTypeRune) {
		return false
	}

	return true
}

func splitPackageTypePath(path string) (string, string) {
	if !isPackageType(path) {
		return "", ""
	}

	return path[:strings.LastIndex(path, "/")], path[strings.LastIndex(path, "/")+1:]
}

func jsonTagInfo(field *ast.Field) (string, bool) {
	var jsonTag, jsonName string
	name := field.Names[0].Name

	if field.Tag != nil && len(strings.TrimSpace(field.Tag.Value)) > 0 {
		tagLiteral, err := strconv.Unquote(field.Tag.Value)
		if err != nil {
			return name, false
		}

		if strings.TrimSpace(tagLiteral) != "" {
			jsonTag = reflect.StructTag(tagLiteral).Get("json")

			// ignore the field entirely
			if jsonTag == "-" {
				return "", true
			}

			if idx := strings.Index(jsonTag, ","); idx != -1 {
				jsonName = jsonTag[:idx]
			} else {
				jsonName = jsonTag
			}

			if jsonName != "" {
				name = jsonName
			}

		}
	}

	return name, false
}
