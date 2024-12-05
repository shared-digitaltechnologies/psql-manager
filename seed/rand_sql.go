package psqlseed

import (
	"context"
	"fmt"
	"io/fs"
	"strings"
	"text/template"

	"github.com/brianvoe/gofakeit/v7"
	"github.com/google/uuid"
	"github.com/shared-digitaltechnologies/psql-manager/db"
	"github.com/shared-digitaltechnologies/psql-manager/seed/fake"
)

type TemplateSeeder interface {
	TemplateString() (string, error)
}

type templateSeeder struct {
	name        string
	templateSrc TemplateSeeder
	sql         <-chan string
}

func (v *templateSeeder) Name() string {
	return v.name
}

func (v *templateSeeder) Id() uuid.UUID {
	return uuid.NewSHA1(RandSeederSqlIdNs, []byte(v.Name()))
}

func (v *templateSeeder) String() string {
	return v.name
}

func (v *templateSeeder) Prepare(ctx context.Context, seed fake.Seed) {
	c := make(chan string)
	v.sql = c
	go func() {
		defer close(c)

		faker := seed.NewFaker([]byte(v.name))

		templ, err := v.templateSrc.TemplateString()
		if err != nil {
			panic(err)
		}

		sql, err := faker.Template(templ, &gofakeit.TemplateOptions{
			Funcs: template.FuncMap{
				"loop": func(from, to int) <-chan int {
					ch := make(chan int)

					go func() {
						for i := from; i <= to; i++ {
							ch <- i
						}
						close(ch)
					}()
					return ch
				},
				"quoteEscape": func(val string) string {
					return strings.ReplaceAll(val, `'`, `''`)
				},
			},
		})
		if err != nil {
			panic(err)
		}

		c <- sql
	}()
}

func (v *templateSeeder) RunSeederTx(ctx context.Context, seed fake.Seed, tx db.Tx) error {
	var err error
	for sql := range v.sql {
		if err == nil {
			fmt.Println(sql)
			_, err = tx.Exec(ctx, sql)
			if err != nil {
				err = db.ErrWithPgRowCol(err, v.Name(), sql)
			}
		}
	}

	return err
}

type randSqlSeeder struct {
	template string
}

func RandSqlSeeder(name string, template string) Seeder {
	return &templateSeeder{
		name: name,
		templateSrc: &randSqlSeeder{
			template: template,
		},
	}
}

func (v *randSqlSeeder) TemplateString() (string, error) {
	return v.template, nil
}

type randSqlFileSeeder struct {
	fsys     fs.FS
	filename string
}

func RandSqlFileSeeder(fsys fs.FS, filename string) Seeder {
	return &templateSeeder{
		name: filename,
		templateSrc: &randSqlFileSeeder{
			fsys:     fsys,
			filename: filename,
		},
	}
}

func (v *randSqlFileSeeder) TemplateString() (string, error) {
	bytes, err := fs.ReadFile(v.fsys, v.filename)
	if err != nil {
		return "", err
	}

	return string(bytes), nil
}
