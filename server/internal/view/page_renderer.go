package view

import (
	"fmt"
	"html/template"
	"io"
)

type PageRenderer struct {
	templates map[string]*template.Template
}

func NewPageRenderer(tmplMap map[string][]string) *PageRenderer {
	templates := make(map[string]*template.Template)

	for k, v := range tmplMap {
		t := template.Must(template.ParseFiles(v...))
		templates[k] = t
	}
	return &PageRenderer{templates: templates}
}

func (pr *PageRenderer) RenderTemplate(wr io.Writer, name string, data any) error {
	if t, ok := pr.templates[name]; ok {
		return t.ExecuteTemplate(wr, name, data)
	}
	return fmt.Errorf("Template is missing{%s}", name)
}
