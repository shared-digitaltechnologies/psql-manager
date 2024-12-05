package psqlseed

import (
	"io/fs"
	"path/filepath"
	"strings"
)

func FileSeeder(fsys fs.FS, filename string) Seeder {
	if strings.HasSuffix(filename, ".tmpl") {
		return RandSqlFileSeeder(fsys, filename)
	} else {
		return DetrSqlFileSeeder(fsys, filename)
	}
}

func (s *Repository) AddDir(fsys fs.FS, path ...string) (err error) {
	var entries []fs.DirEntry
	if len(path) > 0 {
		dirname := filepath.Join(path...)
		entries, err = fs.ReadDir(fsys, dirname)
		if err != nil {
			return
		}

		fsys, err = fs.Sub(fsys, dirname)
		if err != nil {
			return
		}
	} else {
		entries, err = fs.ReadDir(fsys, ".")
		if err != nil {
			return err
		}
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		filename := entry.Name()
		s.Add(FileSeeder(fsys, filename))
	}

	return nil
}

func AddDir(fsys fs.FS, path ...string) error {
	return globalRepository.AddDir(fsys, path...)
}
