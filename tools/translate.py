"""Fait compléter par le modèle les parties que prepare.py n'a pas traduites.

Seules les tables au statut « todo » passent ici : leur query.sql est déjà
écrit, sauf les CTE marquées TODO(model). Pour chacune :
  1. opencode run --agent translator, session neuve, avec son prompt.md
     (le brouillon SQL + le code Qlik + les faits) ;
  2. check.py sur le query.sql complété ;
  3. si erreurs : UNE relance avec la liste exacte des erreurs (retry.md) ;
  4. statut mis à jour dans le manifest et dans journal.csv.

Reprise naturelle : relancer la commande ne traite que les sections « todo »
et « erreur_modele ». Les « escalade » ne sont jamais retraduites
automatiquement (ta correction manuelle serait écrasée) : --retry-escalated
pour le faire exprès. Les « deterministic » ne passent jamais par le modèle.

Usage :
    python tools/translate.py work/ventes
    python tools/translate.py work/ventes --only 012_lot --force
    python tools/translate.py work/ventes --limit 3      # pilote
    python tools/translate.py work/ventes --dry-run
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import shutil
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import journal  # noqa: E402
from check import check_section, get_parser  # noqa: E402

MESSAGE = ("Read the file {prompt} and follow its instructions exactly. "
           "Write the complete query to {out} and nothing else.")
RETRY = ("Read the file {prompt}, then the file {retry}: it lists the errors "
         "found in your previous {out}. Rewrite {out} fixing ALL of them.")


def run_opencode(message: str, args) -> tuple[bool, str]:
    cmd = [args.opencode_bin, "run", "--agent", args.agent]
    if args.model:
        cmd += ["--model", args.model]
    cmd.append(message)
    if args.dry_run:
        print("    $", " ".join(cmd[:-1]), repr(message))
        return True, ""
    try:
        res = subprocess.run(cmd, capture_output=True, text=True,
                             timeout=args.timeout)
    except subprocess.TimeoutExpired:
        return False, f"timeout après {args.timeout}s"
    return res.returncode == 0, (res.stdout + res.stderr)[-1500:]


def translate_section(workdir: Path, sec: dict, args,
                      parser=None) -> tuple[str, int, str]:
    """Le modèle complète les TODO(model) de query.sql (une relance)."""
    d = workdir / sec["id"]
    out, prompt, retry = d / "query.sql", d / "prompt.md", d / "retry.md"
    draft = out.read_text(encoding="utf-8") if out.exists() else ""
    retry.unlink(missing_ok=True)
    errors: list[str] = []
    for attempt in range(1, args.retries + 2):
        if attempt == 1:
            msg = MESSAGE.format(prompt=prompt.as_posix(), out=out.as_posix())
        else:
            retry.write_text(
                "# Errors found in your previous query.sql\n\n"
                + "\n".join(f"- {e}" for e in errors)
                + "\n\nFix every error. Keep everything that was correct.\n",
                encoding="utf-8")
            msg = RETRY.format(prompt=prompt.as_posix(),
                               retry=retry.as_posix(), out=out.as_posix())
        ok, log = run_opencode(msg, args)
        if args.dry_run:
            return "dry-run", attempt, ""
        if not ok:
            out.write_text(draft, encoding="utf-8")      # brouillon intact
            last = log.strip().splitlines()
            return "erreur_modele", attempt, last[-1] if last else "échec opencode"
        errors, _ = check_section(sec, out, parser)
        if not errors:
            return "ok", attempt, ""
        print(f"    {sec['id']} ✗ tentative {attempt} : {len(errors)} erreur(s)")
    return "escalade", args.retries + 1, " | ".join(errors)[:500]


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("workdir", type=Path, help="work/<script>")
    p.add_argument("--only", help="id d'une section")
    p.add_argument("--limit", type=int, help="nombre max de sections")
    p.add_argument("--force", action="store_true",
                   help="retraduire même les sections déjà ok")
    p.add_argument("--retry-escalated", action="store_true",
                   help="retraduire aussi les sections en escalade "
                        "(écrase leur query.sql)")
    p.add_argument("--retries", type=int, default=1)
    p.add_argument("--jobs", type=int, default=1,
                   help="tables traduites en parallèle (selon ton serveur)")
    p.add_argument("--agent", default="translator")
    p.add_argument("--model", help="provider/model")
    p.add_argument("--timeout", type=int, default=600)
    p.add_argument("--opencode-bin", default="opencode")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args(argv)

    mpath = args.workdir / "manifest.json"
    manifest = json.loads(mpath.read_text(encoding="utf-8"))
    journal_path = args.workdir.parent / "journal.csv"
    todo = [s for s in manifest["sections"]
            if (args.only is None or s["id"] == args.only)
            and (s["status"] in ("todo", "erreur_modele")
                 or (args.retry_escalated and s["status"] == "escalade")
                 or (args.force and s["status"] == "ok"))]
    if args.limit:
        todo = todo[:args.limit]
    print(f"{len(todo)} table(s) à compléter par le modèle")
    if todo and not args.dry_run and not shutil.which(args.opencode_bin):
        print(f"'{args.opencode_bin}' introuvable dans le PATH")
        return 1

    # parseur Spark partagé : une erreur de syntaxe déclenche la relance
    parser = None if args.dry_run or not todo else get_parser()

    def work(sec):
        t0 = time.perf_counter()
        result = translate_section(args.workdir, sec, args, parser)
        return sec, result, round(time.perf_counter() - t0)

    t_all = time.perf_counter()
    with ThreadPoolExecutor(max_workers=max(1, args.jobs)) as pool:
        futures = []
        for sec in todo:
            print(f"-> {sec['id']} (~{sec.get('prompt_tokens', '?')} tokens)")
            futures.append(pool.submit(work, sec))
        for fut in as_completed(futures):
            sec, (status, attempts, problem), secs = fut.result()
            print(f"   {sec['id']} : {status} en {secs}s")
            if args.dry_run:
                continue
            sec["status"] = status
            mpath.write_text(json.dumps(manifest, indent=2, ensure_ascii=False),
                             encoding="utf-8")
            journal.update(journal_path, {
                "date": dt.datetime.now().strftime("%Y-%m-%d %H:%M"),
                "script": manifest["script"], "section": sec["id"],
                "status": status, "attempts": attempts, "seconds": secs,
                "problem": problem})

    counts = {}
    for s in manifest["sections"]:
        counts[s["status"]] = counts.get(s["status"], 0) + 1
    print(f"\nTerminé en {round(time.perf_counter() - t_all)}s : {counts}")
    print(f"Journal : {journal_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
