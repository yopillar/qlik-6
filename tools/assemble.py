"""Range un script traduit dans le projet.

Les requêtes vont dans src/<nom-projet>/sql/, et la description du script
(entrées, étapes, sorties) dans work/<script>/assembly.json, qu'orchestrate.py
relit ensuite pour écrire le job.

Le job lit et écrit par nom, avec read_input() et write_output() de
spark-foundation-core : chaque fichier lu ou écrit par le script doit donc
être déclaré dans conf/io.yml. Sinon on refuse d'assembler, plutôt que de
produire un job qui plantera au premier read_input().

Chaque section validée (statut ok ou deterministic) donne un fichier
src/<nom-projet>/sql/<script>__NN_<table>.sql, numéroté dans l'ordre du
script. Le préfixe <script>__ est là parce que tous les scripts partagent le
même dossier sql/ et que deux scripts peuvent très bien avoir une table du
même nom. Le modèle n'intervient pas ici, ce n'est que de la copie.

Usage :
    python tools/assemble.py work/ventes
    python tools/assemble.py work/ventes --partial   # saute les tables non validées
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from job_template import project_dir  # noqa: E402


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("workdir", type=Path, help="work/<script>")
    p.add_argument("--partial", action="store_true",
                   help="assembler même si des tables ne sont pas validées")
    args = p.parse_args(argv)

    manifest = json.loads((args.workdir / "manifest.json").read_text(
        encoding="utf-8"))
    secs = [s for s in manifest["sections"] if s["status"] != "skip"]
    pending = [s["id"] for s in secs
               if s["status"] not in ("ok", "deterministic")]
    if pending and not args.partial:
        print(f"{len(pending)} table(s) non validée(s) : {pending[:8]}…\n"
              "Assemblage annulé (--partial pour forcer).")
        return 1

    stem = Path(manifest["script"]).stem
    out = project_dir() / "sql"

    # On vérifie tout avant de copier quoi que ce soit, pour ne pas laisser
    # un script à moitié assemblé dans sql/.
    inputs, read_paths, undeclared = {}, set(), []
    for s in secs:
        if s["id"] in pending:
            continue
        for view, src in s.get("views", {}).items():
            if "path" in src:
                read_paths.add(src["path"])
                if src.get("io"):
                    inputs[view] = src["io"]
                else:
                    undeclared.append(f"entrée {Path(src['path']).name}")
        if s.get("outputs", {}).get(s["table"]) and \
                not s.get("output_io", {}).get(s["table"]):
            undeclared.append(f"sortie {Path(s['outputs'][s['table']]).name}")
    if undeclared:
        print(f"{stem} : pas déclarés dans conf/io.yml, assemblage annulé : "
              f"{', '.join(sorted(set(undeclared)))}\n"
              "  (python tools/fill_conf.py script_qvd/ les ajoute)")
        # un ancien assembly.json ferait entrer une version périmée du script
        # dans le job
        (args.workdir / "assembly.json").unlink(missing_ok=True)
        return 1

    out.mkdir(parents=True, exist_ok=True)
    for old in out.glob(f"{stem}__*.sql"):
        old.unlink()
    steps, k = [], 0
    for s in secs:
        if s["id"] in pending:
            continue
        k += 1
        name = f"{stem}__{k:02d}_{s['id'].split('_', 1)[1]}.sql"
        shutil.copyfile(args.workdir / s["id"] / "query.sql", out / name)
        output = s.get("outputs", {}).get(s["table"])
        # Le script relit-il le fichier qu'il écrit ? Le job devra alors
        # couper la lignée avant d'écrire.
        steps.append([name, s["view"], s.get("output_io", {}).get(s["table"]),
                      output in read_paths])

    if not any(st[2] for st in steps):
        print("⚠ aucune sortie : aucun STORE du script n'a été rattaché à une "
              "table (voir les notes de work/<script>/rapport.md)")
    (args.workdir / "assembly.json").write_text(json.dumps(
        {"script": manifest["script"], "inputs": dict(sorted(inputs.items())),
         "steps": steps, "partial": bool(pending)},
        indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"{stem} : {len(steps)} requête(s) dans {out}/"
          + (f", {len(pending)} ignorée(s)" if pending else ""))
    return 0


if __name__ == "__main__":
    sys.exit(main())
