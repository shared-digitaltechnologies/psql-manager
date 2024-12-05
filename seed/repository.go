package psqlseed

import (
	"fmt"
	"sort"
	"strings"
)

type Repository struct {
	seeders []Seeder
}

var globalRepository Repository

// Copy returns a copy of the seeder repository. It copies only the list
// of references to the seeders, not the seeders themselves.
//
// Uses the global repository if called on the `nil` repository.
func (s *Repository) Copy() *Repository {
	if s == nil {
		s = &globalRepository
	}

	seeders := make([]Seeder, len(s.seeders))
	copy(seeders, s.seeders)

	return &Repository{
		seeders: seeders,
	}
}

func (s *Repository) Add(seeders ...Seeder) {
	s.seeders = append(s.seeders, seeders...)
}

func Add(seeders ...Seeder) {
	globalRepository.Add(seeders...)
}

func (s *Repository) Seeders() []Seeder {
	if s == nil {
		s = &globalRepository
	}

	s.Sort()
	return s.seeders
}

func Seeders() []Seeder {
	return globalRepository.Seeders()
}

func (st *Repository) Sort() {
	if st == nil {
		st = &globalRepository
	}

	s := st.seeders
	sort.Slice(s, func(i, j int) bool {
		return s[i].Name() < s[j].Name()
	})
}

func (s *Repository) SubStore(seederNames ...string) (store Repository, err error) {
	if s == nil {
		s = &globalRepository
	}

	resultSeeders := make([]Seeder, len(seederNames))
	allSeeders := s.seeders

	var notFound []string

names:
	for i, name := range seederNames {
		for _, seeder := range allSeeders {
			if name == seeder.Name() {
				resultSeeders[i] = seeder
				continue names
			}
		}

		notFound = append(notFound, name)
	}

	store = Repository{seeders: resultSeeders}

	if len(notFound) > 0 {
		err = fmt.Errorf("Seeders '%s' not found", strings.Join(notFound, "', '"))
	}
	return
}

func (s *Repository) SubStoreTill(namePrefix string) Repository {
	if s == nil {
		s = &globalRepository
	}

	allSeeders := s.Seeders()

	till := len(allSeeders)
	for i, seeder := range allSeeders {
		name := seeder.Name()
		subname := name[0:len(namePrefix)]

		if strings.Compare(subname, namePrefix) > 0 {
			till = i + 1
			break
		}
	}

	return Repository{seeders: allSeeders[:till]}
}

func SubStoreTill(namePrefix string) Repository {
	return globalRepository.SubStoreTill(namePrefix)
}

func SubStore(seederNames ...string) (Repository, error) {
	return globalRepository.SubStore(seederNames...)
}
