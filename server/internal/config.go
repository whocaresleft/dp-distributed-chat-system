package internal

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
)

type Config struct {
	FolderPath          string `json:"folder-path"`
	NodeId              uint64 `json:"node-id"`
	ControlPlanePort    uint16 `json:"control-plane-port"`
	DataPlanePort       uint16 `json:"data-plane-port"`
	EnableLogging       bool   `json:"enable-logging"`
	BootstrapServerAddr string `json:"bootstrap-server-addr"`
	DBName              string `json:"db-name"`
	HTTPServerPort      uint16 `json:"http-server-port"`
	TemplateDirectory   string `json:"template-directory"`
	ReadTimeout         int64  `json:"read-timeout"`
	WriteTimeout        int64  `json:"write-timeout"`
	SecretKey           string `json:"secret-key"`
}

func LoadConfig(folderPath string) (*Config, error) {

	file, err := os.OpenFile(folderPath+"/.cfg", os.O_RDONLY, 0755)
	if err != nil {
		return nil, err
	}
	payload, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}

	var config *Config = &Config{}
	if err = json.Unmarshal(payload, config); err != nil {
		return nil, err
	}

	return config, nil
}

func RetrieveWebTemplates(templateDir string) (map[string][]string, error) {

	mapping := make(map[string][]string)

	layoutPath := filepath.Join(templateDir, "layouts")
	layoutFiles, err := filepath.Glob(filepath.Join(layoutPath, "*.html"))
	if err != nil {
		return nil, err
	}

	pageFiles, err := filepath.Glob(filepath.Join(templateDir, "*.html"))
	if err != nil {
		return nil, err
	}

	for _, page := range pageFiles {
		files := append([]string{}, layoutFiles...)
		files = append(files, page)
		mapping[filepath.Base(page)] = files
	}

	return mapping, nil
}
