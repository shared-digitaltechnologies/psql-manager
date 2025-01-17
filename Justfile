test:
    go test

is-clean:
    #!/usr/bin/env bash
    if [[ ! -z "$(git status --porcelain)" ]]; then
      echo "Git not clean!" 1>&2
      exit 1;
    fi

release kind: is-clean test
    #!/usr/bin/env bash
    version="$(semver {{ kind }})"
    git tag -a "$version" -m "Release $version"
    git push origin "$version"
