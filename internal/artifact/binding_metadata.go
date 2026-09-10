package artifact

import (
	"runtime/debug"
	"sort"
)

type object = map[string]any

func SBOM(m LocalEvidence) object {
	components := []object{}
	dependencies := []object{}
	seen := make(map[string]bool)
	for _, a := range m.Artifacts {
		components = append(components, object{"type": "application", "bom-ref": a.Name, "name": a.Name, "version": m.Source.Commit, "hashes": []object{{"alg": "SHA-256", "content": a.SHA256}}})
		refs := []string{}
		mods := append([]*debug.Module{{Path: "stdlib", Version: a.Build.GoVersion}}, a.Build.Deps...)
		for _, dep := range mods {
			if dep.Replace != nil {
				dep = dep.Replace
			}
			ref := dep.Path + "@" + dep.Version
			refs = append(refs, ref)
			if seen[ref] {
				continue
			}
			seen[ref] = true
			component := object{"type": "library", "bom-ref": ref, "name": dep.Path, "version": dep.Version}
			if dep.Sum != "" {
				component["properties"] = []object{{"name": "go:module:sum", "value": dep.Sum}}
			}
			components = append(components, component)
		}
		dependencies = append(dependencies, object{"ref": a.Name, "dependsOn": refs})
	}
	// Bind the exact embedded set, including historical six-binary releases.
	for _, a := range m.Artifacts[2:] {
		dependencies[0]["dependsOn"] = append(dependencies[0]["dependsOn"].([]string), a.Name)
	}
	return object{"bomFormat": "CycloneDX", "specVersion": "1.6", "version": 1, "components": components, "dependencies": dependencies}
}

func Provenance(m LocalEvidence) object {
	subjects := []object{}
	for _, a := range m.Artifacts {
		subjects = append(subjects, object{"name": a.Name, "digest": object{"sha256": a.SHA256}})
	}
	return object{"_type": "https://in-toto.io/Statement/v1", "subject": subjects, "predicateType": "https://slsa.dev/provenance/v1", "predicate": object{
		"buildDefinition": object{"buildType": "https://github.com/CIPFZ/rdev/local-release-gate/v1", "externalParameters": object{"source": m.Source, "command": "make release-gate"}, "internalParameters": object{"assurance": "local unsigned evidence; no hosted CI execution or signing claim"}, "resolvedDependencies": []object{{"uri": "git+https://github.com/CIPFZ/rdev", "digest": object{"gitCommit": m.Source.Commit, "sha256": m.Source.TreeSHA256}}}},
		"runDetails":      object{"builder": object{"id": "https://github.com/CIPFZ/rdev/local-release-gate/v1"}, "metadata": object{}, "byproducts": evidence(m.Evidence)},
	}}
}

func evidence(e map[string]string) []object {
	names := make([]string, 0, len(e))
	for name := range e {
		names = append(names, name)
	}
	sort.Strings(names)
	out := []object{}
	for _, name := range names {
		out = append(out, object{"name": name, "digest": object{"sha256": e[name]}})
	}
	return out
}
