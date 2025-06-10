package generator

import (
	"fmt"
	"go/ast"
	"go/doc"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/datazip-inc/olake/utils/jsonschema/schema"
	"github.com/datazip-inc/olake/utils/logger"
)

func (g *JSONSchemaGenerator) shouldReturnRef(decl *declInfo) bool {
	if decl == nil {
		return false
	}

	if decl.isRoot {
		return false
	}

	ch, _ := utf8.DecodeRuneInString(decl.typeSpec.Name.Name)
	// if the type isn't exported, don't retrun a ref
	if !unicode.IsUpper(ch) {
		return false
	}

	declAnno, _ := g.findJSONSchemaAnnotationForDecl(decl)

	// if we're going to create a $ref, we can skip the title
	if g.options.AutoCreateDefs || (declAnno != nil && declAnno.definition != "") {
		return true
	}

	return false
}

func (g *JSONSchemaGenerator) getDefinitionKey(decl *declInfo) string {
	var defKey string

	declAnno, _ := g.findJSONSchemaAnnotationForDecl(decl)

	if declAnno != nil && declAnno.definition != "" {
		defKey = g.options.DefinitionPrefix + declAnno.definition
	}

	if defKey == "" {
		defKey = g.options.DefinitionPrefix + decl.pkg.Pkg.Path() + "/" + decl.typeSpec.Name.Name
	}

	if strings.Contains(defKey, "/vendor/") {
		defKey = defKey[strings.LastIndex(defKey, "/vendor/")+8:]
	}

	defKey = strings.Replace(defKey, ".", "_", -1)
	defKey = strings.Replace(defKey, "/", "-", -1)

	return defKey
}

func (g *JSONSchemaGenerator) getTitleAndDescriptionForField(decl *declInfo, field *ast.Field) (string, string) {

	var title string
	var desc string

	// if we're going to create a $ref, we can skip the title
	//	if g.shouldReturnRef(decl) {
	//		return "", ""
	//	}

	title, desc = g.getTitleAndDescriptionForObject(decl)
	g.LogDebugF("looking for title/desc for field %s\n", field.Names[0].Name)
	// check if there's an overriding comment on the field itself
	if field.Doc != nil {

		fieldTitle := doc.Synopsis(field.Doc.Text())
		g.LogDebug("Got field doc title", fieldTitle)
		fieldTitleFields := strings.Fields(fieldTitle)
		if len(fieldTitleFields) > 0 && !isIdent(fieldTitleFields[0]) {
			fieldTitle = ""
		}

		if fieldTitle != "" {
			title = fieldTitle
			g.LogVerbose("setting title to ", title)
			fieldDesc := strings.Split(field.Doc.Text()[len(fieldTitle)+1:], "\n\n")[0]
			fieldDescFields := strings.Fields(fieldDesc)

			if len(fieldDescFields) > 0 && !isIdent(fieldDescFields[0]) {
				fieldDesc = ""
			}

			if fieldDesc != "" {
				desc = fieldDesc
			}
		}
	}

	// check if the field has an anno that overrides title
	fieldAnno, _ := g.findJSONSchemaAnnotationForField(field)
	if fieldAnno != nil && fieldAnno.title != "" {
		title = fieldAnno.title
		g.LogVerbose("overriding title with ", title)
	}

	if fieldAnno != nil && fieldAnno.description != "" {
		desc = fieldAnno.description
	}

	return title, desc
}

func (g *JSONSchemaGenerator) getTitleAndDescriptionForObject(decl *declInfo) (string, string) {

	if decl == nil {
		return "", ""
	}
	declAnno, _ := g.findJSONSchemaAnnotationForDecl(decl)

	// first see if there's a doc on the actual object
	var docComment string
	var title string
	var desc string

	if decl.typeSpec.Doc != nil {
		docComment = decl.typeSpec.Doc.Text()
	} else if decl.decl.Doc != nil {
		docComment = decl.decl.Doc.Text()
	}

	if docComment != "" {
		g.LogDebug("found doc comment ", docComment)
		title = doc.Synopsis(docComment)
		desc = strings.Split(docComment[len(title)+1:], "\n\n")[0]

		titleFields := strings.Fields(title)
		descFields := strings.Fields(desc)

		if len(titleFields) > 0 && !isIdent(titleFields[0]) {
			title = ""
		}

		if len(descFields) > 0 && !isIdent(descFields[0]) {
			desc = ""
		}
	}

	// check if the decl's anno overrides title
	if declAnno != nil && declAnno.title != "" {
		title = declAnno.title
	}

	// check if the decl's anno overrides description
	if declAnno != nil && declAnno.description != "" {
		desc = declAnno.description
	}

	return title, desc
}

func (g *JSONSchemaGenerator) addDocsForField(schema schema.JSONSchema, decl *declInfo, field *ast.Field) {
	if field == nil {
		return
	}
	g.LogDebug("adding docs for field ", field.Names[0].Name)
	title, desc := g.getTitleAndDescriptionForField(decl, field)

	if title != "" {
		schema.SetTitle(title)
	}

	if desc != "" {
		schema.SetDescription(desc)
	}
}

func (g *JSONSchemaGenerator) addCommonAttrsForField(schema schema.JSONSchema, field *ast.Field, parentKey string) error {
	if field == nil {
		return nil
	}

	fieldName := field.Names[0].Name
	g.LogVerbose("adding common attrs for field ", fieldName)

	schemaAnno, err := g.findJSONSchemaAnnotationForField(field)

	if err != nil {
		return err
	}

	if schemaAnno == nil {
		return nil
	}

	return g.addCommonAttrs(schema, schemaAnno, fieldName, parentKey)
}

func (g *JSONSchemaGenerator) addCommonAttrsForDecl(schema schema.JSONSchema, decl *declInfo, parentKey string) error {
	if decl == nil {
		return nil
	}

	declName := decl.typeSpec.Name.Name
	g.LogVerbose("adding attrs for type ", declName)
	schemaAnno, err := g.findJSONSchemaAnnotationForDecl(decl)

	if err != nil {
		return err
	}

	if schemaAnno == nil {
		return nil
	}

	return g.addCommonAttrs(schema, schemaAnno, declName, parentKey)
}

func (g *JSONSchemaGenerator) addCommonAttrs(schema schema.JSONSchema, anno *schemaAnno, name string, parentKey string) error {
	if anno.defaultValue != "" {
		schema.SetDefault(anno.defaultValue)
	}

	if anno.constValue != "" {
		schema.SetConstant(anno.constValue)
	}

	if anno.title != "" {
		schema.SetTitle(anno.title)
	}

	if anno.description != "" {
		schema.SetDescription(anno.description)
	}

	if len(anno.enum) > 0 {
		if len(anno.anyOf) > 0 || len(anno.allOf) > 0 || len(anno.oneOf) > 0 {
			return fmt.Errorf("error setting 'enum', one of these present['anyOf', 'allOf', 'oneOf']")
		}
	}

	if len(anno.allOf) > 0 {
		schemas, err := g.generateSchemasFromTypePaths(anno.allOf, parentKey)
		if err != nil {
			return fmt.Errorf("error setting 'allOf' for %s: %s", name, err.Error())
		}
		schema.SetAllOf(schemas)
	}

	if len(anno.anyOf) > 0 {
		schemas, err := g.generateSchemasFromTypePaths(anno.anyOf, parentKey)
		if err != nil {
			return fmt.Errorf("error setting 'anyOf' for %s: %s", name, err.Error())
		}
		schema.SetAnyOf(schemas)
	}

	if len(anno.oneOf) > 0 {
		schemas, err := g.generateSchemasFromTypePaths(anno.oneOf, parentKey)
		if err != nil {
			logger.Error("found error", err)
			return fmt.Errorf("error setting 'oneOf' for %s: %s", name, err.Error())
		}
		schema.SetOneOf(schemas)
	}

	if anno.not != "" {
		schemas, err := g.generateSchemasFromTypePaths([]string{anno.not}, parentKey)
		if err != nil {
			return fmt.Errorf("error setting 'not' for %s: %s", name, err.Error())
		}
		schema.SetNot(schemas[0])
	}

	if anno.order > 0 {
		schema.SetOrder(anno.order)
	}

	return nil
}

func (g *JSONSchemaGenerator) addStringAttrsForField(schema schema.StringSchema, field *ast.Field) error {
	if field == nil {
		return nil
	}

	fieldName := field.Names[0].Name
	g.LogVerbose("adding string attrs for field ", fieldName)
	schemaAnno, err := g.findJSONSchemaAnnotationForField(field)

	if err != nil {
		return err
	}

	if schemaAnno == nil {
		return nil
	}

	g.populateStringAttrs(schema, schemaAnno)

	return nil
}

func (g *JSONSchemaGenerator) addNumericAttrsForField(schema schema.NumericSchema, field *ast.Field) error {
	if field == nil {
		return nil
	}

	fieldName := field.Names[0].Name
	g.LogVerbose("adding numeric attrs for field ", fieldName)
	schemaAnno, err := g.findJSONSchemaAnnotationForField(field)
	g.LogVerbose("error is ", err)
	if err != nil {
		return err
	}

	if schemaAnno == nil {
		return nil
	}

	return g.populateNumericAttrs(schema, schemaAnno)
}

func (g *JSONSchemaGenerator) populateStringAttrs(schema schema.StringSchema, anno *schemaAnno) {

	if schema == nil || anno == nil {
		return
	}

	if anno.format != "" {
		schema.SetFormat(anno.format)
	}

	if anno.pattern != "" {
		schema.SetPattern(anno.pattern)
	}

	if anno.maxLength > -1 {
		schema.SetMaxLength(anno.maxLength)
	}

	if anno.minLength > -1 {
		schema.SetMinLength(anno.minLength)
	}

	if len(anno.enum) > 0 {
		schema.SetEnum(anno.enum)
	}
}

func (g *JSONSchemaGenerator) populateNumericAttrs(schema schema.NumericSchema, anno *schemaAnno) error {
	if schema == nil || anno == nil {
		return nil
	}

	if anno.maximum > -1 {
		schema.SetMaximum(anno.maximum)
		schema.SetExclusiveMaximum(anno.exclusiveMaximum)
	}

	if anno.minimum > -1 {
		schema.SetMinimum(anno.minimum)
		schema.SetExclusiveMinimum(anno.exclusiveMinimum)
	}

	if anno.multipleOf > -1 {
		schema.SetMultipleOf(anno.multipleOf)
	}

	if len(anno.enum) > 0 {
		return schema.SetIntEnum(anno.enum)
	}

	return nil
}

func (g *JSONSchemaGenerator) addArrayAttrsForField(schema schema.ArraySchema, field *ast.Field) error {
	if field == nil {
		return nil
	}

	fieldName := field.Names[0].Name
	g.LogVerbose("adding array attrs for field ", fieldName)
	schemaAnno, err := g.findJSONSchemaAnnotationForField(field)

	if err != nil {
		return err
	}

	if schemaAnno == nil {
		return nil
	}

	if schemaAnno.maxItems > 0 {
		schema.SetMaxItems(schemaAnno.maxItems)
	}

	if schemaAnno.minItems > -1 {
		schema.SetMinItems(schemaAnno.minItems)
	}

	schema.SetAdditionalItems(schemaAnno.additionalItems)
	schema.SetUniqueItems(schemaAnno.uniqueItems)

	return nil
}

func (g *JSONSchemaGenerator) ensureProperTypeForInterfaceField(sch schema.JSONSchema, field *ast.Field) error {
	g.LogVerbose("ensuring proper interface type for field ", field.Names[0].Name)
	schemaAnno, err := g.findJSONSchemaAnnotationForField(field)
	if err != nil {
		return err
	}

	if schemaAnno == nil {
		return nil
	}

	if len(schemaAnno.schemaType) > 0 {
		if len(schemaAnno.schemaType) == 1 {
			g.LogVerbose("found type on anno ", schemaAnno.schemaType)
			sch.SetType(schemaAnno.schemaType[0])
		} else {
			sch.SetType(strings.Join(schemaAnno.schemaType, ","))
		}

	}

	return nil
}

func (g *JSONSchemaGenerator) addObjectAttrsForDecl(sch schema.ObjectSchema, decl *declInfo, parentKey string) error {
	if decl == nil {
		return nil
	}

	title, desc := g.getTitleAndDescriptionForObject(decl)

	if title != "" {
		sch.SetTitle(title)
	}

	if desc != "" {
		sch.SetDescription(desc)
	}

	schemaAnno, err := g.findJSONSchemaAnnotationForDecl(decl)
	if err != nil {
		return err
	}

	if schemaAnno == nil {
		return nil
	}

	if schemaAnno.id != "" {
		sch.SetID(schemaAnno.id)
	}

	if schemaAnno.maxProperties > 0 {
		sch.SetMaxProperties(schemaAnno.maxProperties)
	}

	if schemaAnno.minProperties > -1 {
		sch.SetMinProperties(schemaAnno.minProperties)
	}

	if aprops := schemaAnno.additionalProperties; aprops != nil {
		if aprops.isBool {
			sch.SetAdditionalProperties(schema.NewBoolOrSchema(aprops.boolean))
		} else {
			schemaItem, err := g.generateSchemaFromTypePath(aprops.path, parentKey)
			if err != nil {
				return err
			}
			sch.SetAdditionalProperties(schema.NewBoolOrSchema(schemaItem))
		}
	}
	return g.addCommonAttrs(sch, schemaAnno, decl.typeSpec.Name.Name, parentKey)

}

func (g *JSONSchemaGenerator) fieldIsRequired(field *ast.Field) bool {
	anno, err := g.findJSONSchemaAnnotationForField(field)

	if err != nil {
		return false
	}

	if anno == nil {
		return false
	}

	return anno.required
}

func (g *JSONSchemaGenerator) findJSONSchemaAnnotationForField(field *ast.Field) (*schemaAnno, error) {
	if cachedAnno, found := g.fieldAnnoCache[field]; found {
		return cachedAnno, nil
	}

	docText := field.Doc.Text()
	if docText != "" {
		annos, errs := g.annoParser.Parse(docText)

		if len(errs) > 0 {
			g.LogDebug("got error parsing annotation ", errs[0].Error())
			return nil, fmt.Errorf("error parsing annotation for field %s: %s", field.Names[0].Name, errs[0].Error())
		}

		schemaAnnos := annos.ByName(annotationName)
		if len(schemaAnnos) > 0 {
			g.LogVerbose("found a jsonSchema anno")
			g.fieldAnnoCache[field] = schemaAnnos[0].(*schemaAnno)
			return schemaAnnos[0].(*schemaAnno), nil
		}
	}

	return nil, nil
}

func (g *JSONSchemaGenerator) findJSONSchemaAnnotationForDecl(decl *declInfo) (*schemaAnno, error) {
	if decl.schemaAnnotation != nil || decl.parsedAnnos {
		return decl.schemaAnnotation, nil
	}

	var docComment string

	if decl.typeSpec.Doc != nil {
		docComment = decl.typeSpec.Doc.Text()
	} else if decl.decl.Doc != nil {
		docComment = decl.decl.Doc.Text()
	}

	if docComment != "" {
		annos, errs := g.annoParser.Parse(docComment)

		if len(errs) > 0 {
			return nil, fmt.Errorf("error parsing annotation for object %s: %s", decl.typeSpec.Name.Name, errs[0].Error())
		}

		schemaAnnos := annos.ByName(annotationName)
		if len(schemaAnnos) > 0 {
			g.LogVerbose("found a jsonSchema anno")
			decl.schemaAnnotation = schemaAnnos[0].(*schemaAnno)
		}
	}

	decl.parsedAnnos = true
	return decl.schemaAnnotation, nil
}

func (g *JSONSchemaGenerator) generateSchemasFromTypePaths(paths []string, parentKey string) ([]schema.JSONSchema, error) {
	var schemas []schema.JSONSchema
	var err error

	for _, path := range paths {
		var schemaItem schema.JSONSchema
		defKey := g.defKeyFromPath(path)
		g.LogVerbose("path: ", path)
		g.LogVerbose("defKey: ", defKey)
		if path == "#" || defKey == parentKey {
			g.LogVerbose("got self ref")
			schemaItem = generateSelfRef()
		} else {
			schemaItem, err = g.generateSchemaFromTypePath(path, parentKey)
		}

		if schemaItem != nil {
			schemas = append(schemas, schemaItem)
		} else {
			return nil, fmt.Errorf("unable to generate schema for path '%s': %s", path, err)
		}
	}

	return schemas, err
}

func (g *JSONSchemaGenerator) generateSchemaFromTypePath(path string, parentKey string) (schema.JSONSchema, error) {
	var err error

	var schemaItem schema.JSONSchema
	var tmpSchema schema.JSONSchema
	var typeDecl *declInfo
	var ok bool

	if isIdent(path) {
		g.LogVerboseF("found ident: %s\n", path)
		if tmpSchema, ok, err = g.generateSchemaForBuiltIn(path, nil, parentKey); ok {
			schemaItem = tmpSchema
		}
	} else {
		pkgPath, typeName := splitPackageTypePath(path)
		pkgInfo := g.program.Package(pkgPath)
		if pkgInfo != nil {
			typeDecl, err = g.findDeclInfoForPackage(pkgInfo, nil, typeName)
			if err == nil {
				tmpSchema, err = g.generateSchemaForExpr(typeDecl, typeDecl.typeSpec.Type, nil, parentKey)
				if err == nil {
					schemaItem = tmpSchema
				}
			}
		}
	}

	return schemaItem, err
}

func (g *JSONSchemaGenerator) fieldHasXofAnnotation(field *ast.Field) (bool, error) {
	hasXOf := false

	fieldAnno, err := g.findJSONSchemaAnnotationForField(field)

	if err != nil {
		return false, err
	}

	if fieldAnno != nil {
		hasXOf = g.hasXofAnnotation(fieldAnno)
	}

	return hasXOf, nil
}

func (g *JSONSchemaGenerator) declHasXofAnnotation(decl *declInfo) (bool, error) {
	hasXOf := false

	declAnno, err := g.findJSONSchemaAnnotationForDecl(decl)

	if err != nil {
		return hasXOf, err
	}

	if declAnno != nil {
		hasXOf = g.hasXofAnnotation(declAnno)
	}

	return hasXOf, nil
}

func (g *JSONSchemaGenerator) declHasSchemaAnnotation(decl *declInfo) (bool, error) {
	has := false

	declAnno, err := g.findJSONSchemaAnnotationForDecl(decl)

	if err != nil {
		return has, err
	}

	if declAnno != nil {
		has = true
	}

	return has, nil
}

func (g *JSONSchemaGenerator) hasXofAnnotation(anno *schemaAnno) bool {

	if anno != nil {
		if anno.allOf != nil && len(anno.allOf) > 0 {
			return true
		} else if anno.anyOf != nil && len(anno.anyOf) > 0 {
			return true
		} else if anno.oneOf != nil && len(anno.oneOf) > 0 {
			return true
		}
	}

	return false
}
