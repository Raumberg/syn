package dsl

// Node represents a basic AST element
type Node interface {
	// GetNodeType returns the node type
	GetNodeType() string
}

// Program represents the root node of the program
type Program struct {
	Statements []Node
}

func (p *Program) GetNodeType() string {
	return "Program"
}

// FromStatement represents a FROM operator
type FromStatement struct {
	Dataset string
	Block   *Block // New: block of instructions related to this dataset
}

func (f *FromStatement) GetNodeType() string {
	return "FromStatement"
}

// WithStatement represents a WITH block
type WithStatement struct {
	Type  string // "CONCURRENCY", "STREAM", etc.
	Value interface{}
	Block *Block
}

func (w *WithStatement) GetNodeType() string {
	return "WithStatement"
}

// Block represents a block of code in curly braces
type Block struct {
	Statements []Node
}

func (b *Block) GetNodeType() string {
	return "Block"
}

// FieldsStatement represents a FIELDS operator
type FieldsStatement struct {
	Fields []string
}

func (f *FieldsStatement) GetNodeType() string {
	return "FieldsStatement"
}

// UsingStatement represents a USING operator
type UsingStatement struct {
	Type  string // "MODEL", "KEY", "URL"
	Value string
}

func (u *UsingStatement) GetNodeType() string {
	return "UsingStatement"
}

// UsingBlock represents a USING block with multiple parameters
type UsingBlock struct {
	Statements []UsingStatement
}

func (u *UsingBlock) GetNodeType() string {
	return "UsingBlock"
}

// FilterStatement represents a FILTER operator
type FilterStatement struct {
	Field    string
	Operator string // "=", ">=", "<", etc.
	Value    interface{}
}

func (f *FilterStatement) GetNodeType() string {
	return "FilterStatement"
}

// FilterBlock represents a FILTER block with multiple conditions
type FilterBlock struct {
	Field      string
	Conditions []FilterStatement
}

func (f *FilterBlock) GetNodeType() string {
	return "FilterBlock"
}

// DatasetMergeStatement represents a MERGE operator
type DatasetMergeStatement struct {
	Datasets []string // List of dataset names to merge
}

func (d *DatasetMergeStatement) GetNodeType() string {
	return "DatasetMergeStatement"
}

// SaveStatement represents a SAVE operator
type SaveStatement struct {
	Filename string
}

func (s *SaveStatement) GetNodeType() string {
	return "SaveStatement"
}

// GenerateStatement represents a GENERATE operator for creating new data with LLM
type GenerateStatement struct {
	SourceField     string   // Source field for generation
	TargetField     string   // Field where the result will be saved
	Model           string   // Model name for generation
	Temperature     float64  // Generation temperature (optional)
	Tokens          int      // Maximum number of tokens (optional)
	PromptTemplates []string // Prompt templates, if used
}

func (g *GenerateStatement) GetNodeType() string {
	return "GenerateStatement"
}

// PromptStatement represents a PROMPT operator for defining a request template
type PromptStatement struct {
	Name       string   // Template name
	Template   string   // Template text
	Fields     []string // Fields used in the template
	PromptType string   // Prompt type: "system" or "user"
}

func (p *PromptStatement) GetNodeType() string {
	return "PromptStatement"
}

// PragmaStatement represents a compiler directive PRAGMA
type PragmaStatement struct {
	Type  string      // Directive type, for example: "AUTOSAVE"
	Value interface{} // Directive value
}

func (p *PragmaStatement) GetNodeType() string {
	return "PragmaStatement"
}
