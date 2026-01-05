package rendering

import (
	"fmt"
	"html/template"
	"io"
)

type PageRenderer struct {
	Templates map[string]*template.Template
}

func NewPageRenderer(mapping map[string][]string) *PageRenderer {
	var templates = make(map[string]*template.Template)

	for key, value := range mapping {
		t := template.Must(template.ParseFiles(value...))
		templates[key] = t
	}
	return &PageRenderer{templates}
}

func (renderer *PageRenderer) RenderTemplate(writer io.Writer, templateName string, data any) error {
	t, ok := renderer.Templates[templateName]

	if !ok {
		return fmt.Errorf("The following template is missing {%s}", templateName)
	}

	return t.ExecuteTemplate(writer, templateName, data)
}
