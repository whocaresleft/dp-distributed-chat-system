/*
 * Copyright (c) 2026 Francesco Biribo'
 *
 * Permission to use, copy, modify, and distribute this software for any purpose with or without fee is hereby granted, provided that the above copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package view

import (
	"fmt"
	"html/template"
	"io"
)

// PageRenderer renderes web pages throuh a set of templates
type PageRenderer struct {
	templates map[string]*template.Template
}

// Creates a page renderer with the given set:
//
//	The key is a template path
//	The value is a set of paths of templates with layouts
func NewPageRenderer(tmplMap map[string][]string) *PageRenderer {
	templates := make(map[string]*template.Template)

	for k, v := range tmplMap {
		t := template.Must(template.ParseFiles(v...))
		templates[k] = t
	}
	return &PageRenderer{templates: templates}
}

// Renders the template with name "name"
// It returns an error if the corresponding template is not present
func (pr *PageRenderer) RenderTemplate(wr io.Writer, name string, data any) error {
	if t, ok := pr.templates[name]; ok {
		return t.ExecuteTemplate(wr, name, data)
	}
	return fmt.Errorf("Template is missing{%s}", name)
}
