package dsl

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// Parser represents a DSL parser
type Parser struct {
	input     string
	tokens    []string
	position  int
	keywords  map[string]bool
	operators map[string]bool
	debug     bool
}

// NewParser creates a new Parser
func NewParser(input string) *Parser {
	return &Parser{
		input: input,
		keywords: map[string]bool{
			"FROM":        true,
			"WITH":        true,
			"FIELDS":      true,
			"USING":       true,
			"FILTER":      true,
			"MODEL":       true,
			"KEY":         true,
			"URL":         true,
			"MERGE":       true,
			"SAVE":        true,
			"GENERATE":    true,
			"PROMPT":      true,
			"SYSTEM":      true,
			"USER":        true,
			"TOKENS":      true,
			"TEMPERATURE": true,
			"PRAGMA":      true,
			"AUTOSAVE":    true,
			"CONCURRENCY": true,
			"STREAM":      true,
		},
		operators: map[string]bool{
			"=":  true,
			">":  true,
			"<":  true,
			">=": true,
			"<=": true,
			"!=": true,
		},
		debug: false,
	}
}

// SetDebug sets debug mode
func (p *Parser) SetDebug(debug bool) {
	p.debug = debug
}

// Tokenize splits the input string into tokens
func (p *Parser) Tokenize() error {
	// Remove comments
	reComment := regexp.MustCompile(`#.*`)
	input := reComment.ReplaceAllString(p.input, "")

	// First extract strings in quotes so as not to break their tokenization
	var stringTokens []string
	reStrings := regexp.MustCompile(`"[^"]*"`)
	input = reStrings.ReplaceAllStringFunc(input, func(match string) string {
		stringTokens = append(stringTokens, match)
		return fmt.Sprintf("__STR_%d__", len(stringTokens)-1)
	})

	// Regular expression for tokenization
	re := regexp.MustCompile(`(?:\s*)(TOKENS|SYSTEM|USER|AS|TO|FROM|WITH|FIELDS|USING|FILTER|MODEL|KEY|URL|CONCURRENCY|STREAM|MERGE|SAVE|GENERATE|PROMPT|TEMPERATURE|\{|\}|=|>=|<=|!=|>|<|;|,|\[|\]|__STR_\d+__|[\w\d\.\/-]+)(?:\s*)`)
	matches := re.FindAllStringSubmatch(input, -1)

	if matches == nil {
		return errors.New("tokenization error: no recognizable tokens found")
	}

	tokens := []string{}
	for _, match := range matches {
		token := strings.TrimSpace(match[1])
		if token != "" {
			// Restore quoted strings
			if strings.HasPrefix(token, "__STR_") {
				index, err := strconv.Atoi(token[6 : len(token)-2])
				if err == nil && index < len(stringTokens) {
					token = stringTokens[index]
				}
			}
			tokens = append(tokens, token)
		}
	}

	// Debug information - output the resulting tokens only in debug mode
	if p.debug {
		fmt.Println("Tokens:", strings.Join(tokens, ", "))
	}

	p.tokens = tokens
	p.position = 0
	return nil
}

// Parse starts parsing and returns an AST
func (p *Parser) Parse() (*Program, error) {
	if err := p.Tokenize(); err != nil {
		return nil, err
	}

	program := &Program{
		Statements: []Node{},
	}

	for !p.isEOF() {
		stmt, err := p.parseStatement()
		if err != nil {
			return nil, err
		}
		program.Statements = append(program.Statements, stmt)
	}

	return program, nil
}

// parseStatement parses a single statement
func (p *Parser) parseStatement() (Node, error) {
	token := p.peekToken()

	switch token {
	case "FROM":
		return p.parseFromStatement()
	case "WITH":
		return p.parseWithStatement()
	case "FIELDS":
		return p.parseFieldsStatement()
	case "USING":
		return p.parseUsingStatement()
	case "FILTER":
		return p.parseFilterStatement()
	case "MERGE":
		return p.parseMergeStatement()
	case "SAVE":
		return p.parseSaveStatement()
	case "GENERATE":
		return p.parseGenerateStatement()
	case "PROMPT":
		return p.parsePromptStatement("user") // For backward compatibility, PROMPT = USER PROMPT
	case "PRAGMA":
		return p.parsePragmaStatement()
	case "SYSTEM":
		// Check if this is the beginning of SYSTEM PROMPT
		p.nextToken() // Skip SYSTEM
		if p.peekToken() == "PROMPT" {
			p.nextToken() // Skip PROMPT
			return p.parsePromptStatement("system")
		}
		return nil, fmt.Errorf("expected PROMPT after SYSTEM, got: %s", p.peekToken())
	case "USER":
		// Check if this is the beginning of USER PROMPT
		p.nextToken() // Skip USER
		if p.peekToken() == "PROMPT" {
			p.nextToken() // Skip PROMPT
			return p.parsePromptStatement("user")
		}
		return nil, fmt.Errorf("expected PROMPT after USER, got: %s", p.peekToken())
	default:
		return nil, fmt.Errorf("unexpected token: %s", token)
	}
}

// parseFromStatement parses FROM statement
func (p *Parser) parseFromStatement() (Node, error) {
	p.nextToken() // Skip FROM

	if p.isEOF() {
		return nil, errors.New("expected dataset name after FROM")
	}

	dataset := p.nextToken()

	var block *Block
	if p.peekToken() == "{" {
		var err error
		block, err = p.parseBlock()
		if err != nil {
			return nil, err
		}
	}

	return &FromStatement{
		Dataset: dataset,
		Block:   block,
	}, nil
}

// parseWithStatement parses WITH statement
func (p *Parser) parseWithStatement() (Node, error) {
	p.nextToken() // Skip WITH

	if p.isEOF() {
		return nil, errors.New("expected setting type after WITH")
	}

	withType := p.nextToken()

	var value interface{}
	var block *Block

	if withType == "CONCURRENCY" {
		if p.isEOF() {
			return nil, errors.New("expected value after WITH CONCURRENCY")
		}

		concurrency, err := strconv.Atoi(p.nextToken())
		if err != nil {
			return nil, fmt.Errorf("incorrect concurrency value: %v", err)
		}
		value = concurrency
	} else if withType == "STREAM" {
		value = true
	} else {
		return nil, fmt.Errorf("unknown WITH type: %s", withType)
	}

	if p.peekToken() == "{" {
		var err error
		block, err = p.parseBlock()
		if err != nil {
			return nil, err
		}
	}

	return &WithStatement{
		Type:  withType,
		Value: value,
		Block: block,
	}, nil
}

// parseFieldsStatement parses FIELDS statement
func (p *Parser) parseFieldsStatement() (Node, error) {
	p.nextToken() // Skip FIELDS

	fields := []string{}

	// If the next token is an opening brace, then it's a field list
	if p.peekToken() == "[" {
		p.nextToken() // Skip [

		for p.peekToken() != "]" {
			if p.isEOF() {
				return nil, errors.New("expected closing brace ]")
			}

			field := p.nextToken()
			fields = append(fields, stripQuotes(field))

			if p.peekToken() == "," {
				p.nextToken() // Skip comma
			}
		}

		p.nextToken() // Skip ]
	} else {
		// Otherwise it's a single field
		field := p.nextToken()
		fields = append(fields, stripQuotes(field))
	}

	return &FieldsStatement{
		Fields: fields,
	}, nil
}

// parseUsingStatement parses USING statement
func (p *Parser) parseUsingStatement() (Node, error) {
	p.nextToken() // Skip USING

	if p.peekToken() == "{" {
		// USING block
		p.nextToken() // Skip {

		block := &UsingBlock{
			Statements: []UsingStatement{},
		}

		for p.peekToken() != "}" {
			if p.isEOF() {
				return nil, errors.New("expected closing brace }")
			}

			usingType := p.nextToken()
			if usingType != "MODEL" && usingType != "KEY" && usingType != "URL" {
				return nil, fmt.Errorf("expected USING type (MODEL, KEY, URL), got: %s", usingType)
			}

			value := p.nextToken()

			block.Statements = append(block.Statements, UsingStatement{
				Type:  usingType,
				Value: stripQuotes(value),
			})
		}

		p.nextToken() // Skip }

		return block, nil
	} else {
		// Single USING
		usingType := p.nextToken()

		if p.isEOF() {
			return nil, errors.New("expected value after USING type")
		}

		value := p.nextToken()

		return &UsingStatement{
			Type:  usingType,
			Value: stripQuotes(value),
		}, nil
	}
}

// parseFilterStatement parses FILTER statement
func (p *Parser) parseFilterStatement() (Node, error) {
	p.nextToken() // Skip FILTER

	if p.isEOF() {
		return nil, errors.New("expected field after FILTER")
	}

	field := p.nextToken()

	if p.peekToken() == "{" {
		// FILTER block
		p.nextToken() // Skip {

		block := &FilterBlock{
			Field:      field,
			Conditions: []FilterStatement{},
		}

		for p.peekToken() != "}" {
			if p.isEOF() {
				return nil, errors.New("expected closing brace }")
			}

			subField := p.nextToken()

			if p.isEOF() {
				return nil, fmt.Errorf("expected operator after %s", subField)
			}

			operator := p.nextToken()
			if !p.isOperator(operator) {
				return nil, fmt.Errorf("expected operator (=, >, <, >=, <=, !=), got: %s", operator)
			}

			if p.isEOF() {
				return nil, fmt.Errorf("expected value after %s", operator)
			}

			valueStr := p.nextToken()
			var value interface{} = stripQuotes(valueStr)

			// Try to convert to number
			if num, err := strconv.Atoi(valueStr); err == nil {
				value = num
			}

			block.Conditions = append(block.Conditions, FilterStatement{
				Field:    subField,
				Operator: operator,
				Value:    value,
			})

			if p.peekToken() == ";" {
				p.nextToken() // Skip ;
			}
		}

		p.nextToken() // Skip }

		return block, nil
	} else {
		// Single FILTER
		operator := p.nextToken()
		if !p.isOperator(operator) {
			return nil, fmt.Errorf("expected operator (=, >, <, >=, <=, !=), got: %s", operator)
		}

		if p.isEOF() {
			return nil, fmt.Errorf("expected value after %s", operator)
		}

		valueStr := p.nextToken()
		var value interface{} = stripQuotes(valueStr)

		// Try to convert to number
		if num, err := strconv.Atoi(valueStr); err == nil {
			value = num
		}

		return &FilterStatement{
			Field:    field,
			Operator: operator,
			Value:    value,
		}, nil
	}
}

// parseMergeStatement parses MERGE statement
func (p *Parser) parseMergeStatement() (Node, error) {
	p.nextToken() // Skip MERGE

	datasets := []string{}

	// If the next token is an opening brace, then it's a dataset list
	if p.peekToken() == "[" {
		p.nextToken() // Skip [

		for p.peekToken() != "]" {
			if p.isEOF() {
				return nil, errors.New("expected closing brace ]")
			}

			dataset := p.nextToken()
			datasets = append(datasets, stripQuotes(dataset))

			if p.peekToken() == "," {
				p.nextToken() // Skip comma
			}
		}

		p.nextToken() // Skip ]
	} else {
		// Otherwise it's two datasets separated by a comma
		dataset1 := p.nextToken()
		datasets = append(datasets, stripQuotes(dataset1))

		if p.peekToken() == "," {
			p.nextToken() // Skip comma
			dataset2 := p.nextToken()
			datasets = append(datasets, stripQuotes(dataset2))
		} else {
			return nil, errors.New("expected comma between datasets in MERGE")
		}
	}

	if len(datasets) < 2 {
		return nil, errors.New("at least two datasets are required for MERGE")
	}

	return &DatasetMergeStatement{
		Datasets: datasets,
	}, nil
}

// parseSaveStatement parses SAVE statement
func (p *Parser) parseSaveStatement() (Node, error) {
	p.nextToken() // Skip SAVE

	if p.isEOF() {
		return nil, errors.New("expected filename after SAVE")
	}

	filename := p.nextToken()

	return &SaveStatement{
		Filename: stripQuotes(filename),
	}, nil
}

// parseBlock parses a block of code in curly braces
func (p *Parser) parseBlock() (*Block, error) {
	p.nextToken() // Skip {

	block := &Block{
		Statements: []Node{},
	}

	for p.peekToken() != "}" {
		if p.isEOF() {
			return nil, errors.New("expected closing brace }")
		}

		stmt, err := p.parseStatement()
		if err != nil {
			return nil, err
		}

		block.Statements = append(block.Statements, stmt)
	}

	p.nextToken() // Skip }

	return block, nil
}

// parseGenerateStatement parses GENERATE statement
func (p *Parser) parseGenerateStatement() (Node, error) {
	p.nextToken() // Skip GENERATE

	// Check GENERATE format sourceField AS targetField
	if p.isEOF() {
		return nil, errors.New("expected source field after GENERATE")
	}

	sourceField := p.nextToken()

	// Expect keyword AS or TO
	if p.isEOF() || (p.peekToken() != "AS" && p.peekToken() != "TO") {
		return nil, fmt.Errorf("expected 'AS' or 'TO' after source field, got: %s", p.peekToken())
	}

	p.nextToken() // Skip AS or TO

	if p.isEOF() {
		return nil, errors.New("expected target field after AS/TO")
	}

	targetField := p.nextToken()

	// Create base instance with mandatory fields
	generateStmt := &GenerateStatement{
		SourceField: stripQuotes(sourceField),
		TargetField: stripQuotes(targetField),
		Temperature: 0.7,  // Default value
		Tokens:      1024, // Default value
	}

	// If the next token is a block with parameters
	if p.peekToken() == "{" {
		p.nextToken() // Skip {

		// Parse block parameters
		for p.peekToken() != "}" {
			if p.isEOF() {
				return nil, errors.New("expected closing brace }")
			}

			paramType := p.nextToken()

			switch paramType {
			case "MODEL":
				if p.isEOF() {
					return nil, errors.New("expected model name after MODEL")
				}
				generateStmt.Model = stripQuotes(p.nextToken())

			case "TEMPERATURE":
				if p.isEOF() {
					return nil, errors.New("expected value after TEMPERATURE")
				}
				tempStr := p.nextToken()
				tempVal, err := strconv.ParseFloat(tempStr, 64)
				if err != nil {
					return nil, fmt.Errorf("expected numeric value for TEMPERATURE, got: %s", tempStr)
				}
				generateStmt.Temperature = tempVal

			case "TOKENS":
				if p.isEOF() {
					return nil, errors.New("expected value after TOKENS")
				}
				tokensStr := p.nextToken()
				tokensVal, err := strconv.Atoi(tokensStr)
				if err != nil {
					return nil, fmt.Errorf("expected integer value for TOKENS, got: %s", tokensStr)
				}
				generateStmt.Tokens = tokensVal

			case "PROMPT":
				if p.isEOF() {
					return nil, errors.New("expected prompt name after PROMPT")
				}
				promptName := stripQuotes(p.nextToken())
				generateStmt.PromptTemplates = append(generateStmt.PromptTemplates, promptName)

			default:
				return nil, fmt.Errorf("unknown GENERATE parameter: %s", paramType)
			}

			// Check for separator
			if p.peekToken() == ";" {
				p.nextToken() // Skip ;
			}
		}

		p.nextToken() // Skip }
	}

	return generateStmt, nil
}

// parsePromptStatement parses PROMPT statement
func (p *Parser) parsePromptStatement(promptType string) (Node, error) {
	if promptType == "" {
		promptType = "user" // Default to user prompt
	}

	if p.peekToken() == "PROMPT" {
		p.nextToken() // Skip PROMPT if not skipped yet
	}

	if p.isEOF() {
		return nil, errors.New("expected prompt name after PROMPT")
	}

	promptName := p.nextToken()

	// Check if prompt name is followed by block with text or fields
	var promptTemplate string
	var fields []string

	if p.peekToken() == "{" {
		p.nextToken() // Skip {

		// Check next token - is it FIELDS or text template
		if p.peekToken() == "FIELDS" {
			p.nextToken() // Skip FIELDS

			// If next token is an opening brace, then it's a field list
			if p.peekToken() == "[" {
				p.nextToken() // Skip [

				for p.peekToken() != "]" {
					if p.isEOF() {
						return nil, errors.New("expected closing brace ]")
					}

					field := p.nextToken()
					fields = append(fields, stripQuotes(field))

					if p.peekToken() == "," {
						p.nextToken() // Skip comma
					}
				}

				p.nextToken() // Skip ]
			} else {
				// Otherwise it's a single field
				field := p.nextToken()
				fields = append(fields, stripQuotes(field))
			}

			// Expect text template after field list
			if p.isEOF() || p.peekToken() == "}" {
				return nil, errors.New("expected text template after field list")
			}

			// Collect all remaining tokens until } as template
			templateTokens := []string{}
			for p.peekToken() != "}" {
				if p.isEOF() {
					return nil, errors.New("expected closing brace }")
				}

				templateTokens = append(templateTokens, p.nextToken())
			}

			promptTemplate = strings.Join(templateTokens, " ")
			promptTemplate = stripQuotes(promptTemplate)
		} else {
			// Collect all tokens until } as template
			templateTokens := []string{}
			for p.peekToken() != "}" {
				if p.isEOF() {
					return nil, errors.New("expected closing brace }")
				}

				templateTokens = append(templateTokens, p.nextToken())
			}

			promptTemplate = strings.Join(templateTokens, " ")
			promptTemplate = stripQuotes(promptTemplate)
		}

		p.nextToken() // Skip }
	} else {
		// If no block, expect string as template
		if p.isEOF() {
			return nil, errors.New("expected text template")
		}

		promptTemplate = p.nextToken()
		promptTemplate = stripQuotes(promptTemplate)
	}

	return &PromptStatement{
		Name:       stripQuotes(promptName),
		Template:   promptTemplate,
		Fields:     fields,
		PromptType: promptType,
	}, nil
}

// parsePragmaStatement parses compiler PRAGMA directives
func (p *Parser) parsePragmaStatement() (Node, error) {
	p.nextToken() // Skip PRAGMA

	if p.isEOF() {
		return nil, errors.New("expected pragma type after PRAGMA")
	}

	pragmaType := p.nextToken()

	switch pragmaType {
	case "AUTOSAVE":
		return &PragmaStatement{
			Type:  pragmaType,
			Value: true,
		}, nil
	case "CONCURRENCY":
		// Check if after CONCURRENCY follows a number
		if p.isEOF() {
			return nil, errors.New("expected value after PRAGMA CONCURRENCY")
		}

		concurrencyStr := p.nextToken()
		concurrency, err := strconv.Atoi(concurrencyStr)
		if err != nil {
			return nil, fmt.Errorf("expected integer value for PRAGMA CONCURRENCY, got: %s", concurrencyStr)
		}

		return &PragmaStatement{
			Type:  pragmaType,
			Value: concurrency,
		}, nil
	default:
		return nil, fmt.Errorf("unknown PRAGMA directive: %s", pragmaType)
	}
}

// peekToken returns the current token without moving the pointer
func (p *Parser) peekToken() string {
	if p.isEOF() {
		return ""
	}
	return p.tokens[p.position]
}

// nextToken returns the current token and moves the pointer
func (p *Parser) nextToken() string {
	if p.isEOF() {
		return ""
	}
	token := p.tokens[p.position]
	p.position++
	return token
}

// isEOF checks if we've reached the end of tokens
func (p *Parser) isEOF() bool {
	return p.position >= len(p.tokens)
}

// isKeyword checks if a token is a keyword
func (p *Parser) isKeyword(token string) bool {
	_, ok := p.keywords[token]
	return ok
}

// isOperator checks if a token is an operator
func (p *Parser) isOperator(token string) bool {
	_, ok := p.operators[token]
	return ok
}

// stripQuotes removes quotes around a string if they exist
func stripQuotes(s string) string {
	if (strings.HasPrefix(s, "\"") && strings.HasSuffix(s, "\"")) ||
		(strings.HasPrefix(s, "'") && strings.HasSuffix(s, "'")) {
		return s[1 : len(s)-1]
	}
	return s
}
