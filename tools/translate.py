"""Fait compléter par le modèle les parties que prepare.py n'a pas traduites.

Seules les tables au statut « todo » passent ici : leur query.sql est déjà
écrit, sauf les CTE marquées TODO(model). Pour chacune :
  1. opencode run --agent translator, session neuve, avec son prompt.md
     (le brouillon SQL + le code Qlik + les faits) ;
  2. check.py sur le query.sql complété ;
  3. si erreurs : UNE relance, avec sa réponse et la liste exacte des erreurs ;
  4. statut mis à jour dans le manifest et dans journal.csv.

Reprise naturelle : relancer la commande ne traite que les sections « todo »
et « erreur_modele ». Les « escalade » ne sont jamais retraduites
automatiquement (ta correction manuelle serait écrasée) : --retry-escalated
pour le faire exprès. Les « deterministic » ne passent jamais par le modèle.

Le prompt est envoyé DANS le message et le modèle répond par un bloc
```sql``` : il n'a besoin d'aucun outil ni d'aucun fichier (tous ses outils
sont coupés). Pour voir ce qu'il a fait : work/<script>/<table>/model_<n>.log
(message et réponse brute) et query.attempt<n>.sql.

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
import re
import shutil
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import journal  # noqa: E402
from check import check_section, get_parser  # noqa: E402

# Le prompt est envoyé DANS le message et le modèle répond par un bloc
# ```sql``` : aucun outil, aucune lecture de fichier, aucune permission à
# demander. Taille max d'un argument de ligne de commande sous Linux : 128 Kio.
MAX_INLINE = 120_000
RETRY_TEXT = """

# Your previous answer had errors

```sql
{previous}
```

Errors found:
{errors}

Views you may use: {allowed}, plus the CTEs defined in the query. Do not
rename the existing CTEs. Fix every error, keep everything that was correct,
and answer again with the complete query in one ```sql block."""


def extract_sql(text: str) -> str | None:
    """Requête dans la réponse du modèle : le plus long bloc ```sql```, ou à
    défaut le texte à partir de la première ligne SQL."""
    text = text.split("\n--- stderr ---\n")[0]
    blocks = re.findall(r"```(?:sql)?\s*\n(.*?)```", text, re.S | re.I)
    blocks = [b for b in blocks if re.search(r"\bSELECT\b", b, re.I)]
    if blocks:
        return max(blocks, key=len)
    # pas de balises : la requête commence à la première ligne SQL
    m = re.search(r"^\s*(--|WITH\b|SELECT\b)", text, re.M | re.I)
    if m and re.search(r"\bSELECT\b", text[m.start():], re.I):
        return text[m.start():].strip()
    return None


def shrink(msg: str) -> str:
    """Allège un message trop long en retirant les skills."""
    for marker in ("\n# Skill: ",):
        if len(msg.encode("utf-8")) <= MAX_INLINE:
            break
        head, sep, tail = msg.partition(marker)
        if sep:
            # on retire la rubrique jusqu'à la rubrique suivante non concernée
            rest = re.split(r"\n# (?!Skill: )(?=\S)", tail, maxsplit=1)
            msg = head + ("\n# " + rest[1] if len(rest) > 1 else "")
    return msg


def parse_events(stdout: str) -> dict:
    """Lit le flux d'événements de « opencode run --format json » (une ligne
    JSON par événement) : texte final, outils tentés, raisonnement, erreurs.
    Le schéma varie selon les versions : lecture volontairement tolérante."""
    found = {"text": [], "tools": [], "reasoning": False, "errors": [],
             "json": False}

    def walk(obj):
        if isinstance(obj, dict):
            kind = str(obj.get("type", "")).lower()
            if kind == "text" and isinstance(obj.get("text"), str):
                found["text"].append(obj["text"])
            if "reasoning" in kind or "thinking" in kind:
                found["reasoning"] = True
            if isinstance(obj.get("tool"), str):
                state = obj.get("state")
                status = state.get("status") if isinstance(state, dict) else ""
                found["tools"].append(f"{obj.get('tool', '?')} {status}".strip())
            if "error" in kind or (kind == "" and "error" in obj):
                found["errors"].append(str(obj.get("error") or obj)[:300])
            for v in obj.values():
                walk(v)
        elif isinstance(obj, list):
            for v in obj:
                walk(v)

    for line in stdout.splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            event = json.loads(line)
        except ValueError:
            continue
        found["json"] = True
        walk(event)
    found["text"] = "\n".join(dict.fromkeys(found["text"]))
    found["tools"] = list(dict.fromkeys(found["tools"]))
    return found


def run_opencode(message: str, args) -> tuple[bool, str, dict]:
    """Un appel au modèle. Renvoie (succès, sortie brute, événements lus)."""
    cmd = [args.opencode_bin, "run", "--agent", args.agent]
    if args.model:
        cmd += ["--model", args.model]
    if not args.text_output:
        cmd += ["--format", "json"]
    cmd.append(message)
    if args.dry_run:
        print("    $", " ".join(cmd[:-1]), repr(message[:80]) + "…")
        return True, "", {}
    try:
        res = subprocess.run(cmd, capture_output=True, text=True,
                             timeout=args.timeout)
    except subprocess.TimeoutExpired:
        return False, f"timeout après {args.timeout}s", {}
    if res.returncode and not args.text_output and re.search(
            r"format|unknown (option|argument)|unrecognized", res.stderr, re.I):
        # ancienne version d'opencode sans --format json : sortie texte
        args.text_output = True
        print("    (opencode sans --format json : passage en sortie texte)")
        return run_opencode(message, args)
    raw = res.stdout + ("\n--- stderr ---\n" + res.stderr
                        if res.stderr.strip() else "")
    events = parse_events(res.stdout) if not args.text_output else {}
    return res.returncode == 0, raw, events


def translate_section(workdir: Path, sec: dict, args,
                      parser=None) -> tuple[str, int, str]:
    """Le modèle complète les TODO(model) de query.sql (une relance).

    Le prompt est envoyé dans le message ; la requête est lue dans la réponse
    du modèle (bloc ```sql```) puis écrite dans query.sql par ce script.
    Chaque tentative laisse une trace dans le dossier de la table :
      model_<n>.log         commande, message, durée, réponse brute du modèle
      query.attempt<n>.sql  la requête extraite de la réponse
    """
    d = workdir / sec["id"]
    out, prompt_path = d / "query.sql", d / "prompt.md"
    prompt = prompt_path.read_text(encoding="utf-8")
    draft = out.read_text(encoding="utf-8") if out.exists() else ""
    for old in list(d.glob("model_*.log")) + list(d.glob("query.attempt*.sql")) \
            + [d / "retry.md"]:
        old.unlink(missing_ok=True)
    errors: list[str] = []
    previous = draft
    failures = 0
    for attempt in range(1, args.retries + 2):
        msg = prompt
        if attempt > 1:
            msg += RETRY_TEXT.format(
                previous=previous.rstrip(),
                errors="\n".join(f"- {e}" for e in errors),
                allowed=", ".join(sorted(sec.get("views", {}))) or "none")
        msg = shrink(msg)
        if len(msg.encode("utf-8")) > MAX_INLINE:
            # trop long pour la ligne de commande, même allégé : le modèle n'a
            # pas le droit de lire un fichier, la table est à faire à la main
            return "escalade", attempt, ("prompt trop long pour être envoyé "
                                         "dans le message")
        t0 = time.perf_counter()
        ok, log, events = run_opencode(msg, args)
        if args.dry_run:
            return "dry-run", attempt, ""
        answer = events.get("text") if events.get("json") else \
            log.split("\n--- stderr ---\n")[0].strip()
        sql = extract_sql(answer or "") if ok else None
        tools = events.get("tools", [])
        if not ok:
            note = ("opencode a échoué (code retour non nul) : voir la fin "
                    "du log, partie stderr")
        elif not answer and tools:
            note = (f"le modèle a tenté d'utiliser un outil ({', '.join(tools)}) "
                    "au lieu de répondre : outil refusé, pas de texte")
        elif not answer and events.get("reasoning"):
            note = ("le modèle n'a produit que du raisonnement, pas de réponse "
                    "(limite de tokens ?)")
        elif not answer:
            note = "réponse VIDE du modèle (rien sur stdout, voir stderr)"
        elif not sql:
            note = "aucune requête SQL trouvée dans la réponse du modèle"
        else:
            note = ""
        if events.get("errors"):
            note += " | erreurs : " + " ; ".join(events["errors"][:3])
        (d / f"model_{attempt}.log").write_text(
            f"# Tentative {attempt} — {round(time.perf_counter() - t0)}s, "
            f"code retour {'0' if ok else 'erreur'}\n"
            f"# Commande : {args.opencode_bin} run --agent {args.agent}"
            f"{' --model ' + args.model if args.model else ''} <message>\n"
            + (f"# Note     : {note}\n" if note else "")
            + "\n## Message envoyé\n\n" + msg
            + (f"\n\n## Outils tentés\n\n{', '.join(tools)}" if tools else "")
            + "\n\n## Texte de la réponse\n\n" + (answer or "(aucun)")
            + "\n\n## Sortie brute d'opencode\n\n" + log, encoding="utf-8")
        if not ok:
            failures += 1
            errors = ["the previous call failed: answer with the complete query "
                      "in one ```sql block, without writing any file"]
            print(f"    {sec['id']} ✗ tentative {attempt} : {note}")
            continue
        if sql:
            out.write_text(sql, encoding="utf-8")
            (d / f"query.attempt{attempt}.sql").write_text(sql, encoding="utf-8")
            previous = sql
            errors, _ = check_section(sec, out, parser)
        else:
            errors = ["your previous answer contained no SQL. Do not try to "
                      "write files (disabled): reply with plain text, the "
                      "complete query in one ```sql block"]
            print(f"    {sec['id']} : {note}")
        if not errors:
            return "ok", attempt, ""
        print(f"    {sec['id']} ✗ tentative {attempt} : {errors[0][:120]}")
    out.write_text(previous, encoding="utf-8")
    if failures == args.retries + 1:      # aucun appel n'a abouti
        return "erreur_modele", failures, "opencode en échec : voir model_*.log"
    return "escalade", args.retries + 1, " | ".join(errors)[:500]


def selftest(args, quiet: bool = False) -> int:
    """Envoie une question triviale à l'agent et montre ce qui revient.
    quiet : n'affiche rien si tout va bien (contrôle avant traduction)."""
    msg = ("Reply with exactly this text and nothing else, without using any "
           "tool:\n```sql\nSELECT 1 AS ok\n```")
    t0 = time.perf_counter()
    ok, raw, events = run_opencode(msg, args)
    sec = round(time.perf_counter() - t0)
    answer = events.get("text") if events.get("json") else \
        raw.split("\n--- stderr ---\n")[0].strip()
    if quiet and extract_sql(answer or ""):
        return 0
    if quiet:
        print("\n⚠ Le modèle ne répond pas correctement à une question simple :")
    print(f"code retour      : {'0' if ok else 'ERREUR'} ({sec}s)")
    print(f"format json      : {'oui' if events.get('json') else 'non'}")
    print(f"outils tentés    : {', '.join(events.get('tools', [])) or 'aucun'}")
    print(f"raisonnement seul: {'oui' if events.get('reasoning') and not answer else 'non'}")
    print(f"texte reçu       : {answer[:300] if answer else '(aucun)'}")
    print(f"SQL extrait      : {extract_sql(answer or '') or '(aucun)'}")
    if not answer:
        print("\n--- sortie brute ---\n" + raw[-3000:])
    return 0 if extract_sql(answer or "") else 1


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("workdir", type=Path, nargs="?", default=Path("work"),
                   help="work/<script>")
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
    p.add_argument("--text-output", action="store_true",
                   help="ne pas utiliser --format json (anciennes versions)")
    p.add_argument("--selftest", action="store_true",
                   help="vérifier que le modèle répond bien, sans rien traduire")
    p.add_argument("--skip-selftest", action="store_true",
                   help="ne pas vérifier le modèle avant de traduire")
    args = p.parse_args(argv)

    if args.selftest:
        return selftest(args)

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

    if todo and not args.dry_run and not args.skip_selftest:
        if selftest(args, quiet=True):
            print("\nTraduction arrêtée : corrige d'abord ce problème "
                  "(modèle, agent, permissions), puis relance.\n"
                  "Test seul : python tools/translate.py --selftest")
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
