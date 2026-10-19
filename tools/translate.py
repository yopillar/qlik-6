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
from prepare import fill_holes, hole_key, load_cache  # noqa: E402

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
             "json": False, "tool_inputs": [], "finish": None,
             "output_tokens": None}

    def walk(obj):
        if isinstance(obj, dict):
            kind = str(obj.get("type", "")).lower()
            if kind == "text" and isinstance(obj.get("text"), str):
                found["text"].append(obj["text"])
            if "reasoning" in kind or "thinking" in kind:
                found["reasoning"] = True
            if isinstance(obj.get("reason"), str) and "finish" in kind:
                found["finish"] = obj["reason"]
            tokens = obj.get("tokens")
            if isinstance(tokens, dict) and isinstance(tokens.get("output"), int):
                found["output_tokens"] = tokens["output"]
            if isinstance(obj.get("tool"), str):
                state = obj.get("state")
                status = state.get("status") if isinstance(state, dict) else ""
                found["tools"].append(f"{obj.get('tool', '?')} {status}".strip())
                # contenu que le modèle voulait écrire (write / edit refusés)
                inp = state.get("input") if isinstance(state, dict) else None
                if isinstance(inp, dict):
                    for key in ("content", "newString", "new_string", "text",
                                "patch", "code"):
                        if isinstance(inp.get(key), str):
                            found["tool_inputs"].append(inp[key])
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


# ---------------------------------------------------------------------------
# Greffe des CTE du modèle dans le brouillon : le modèle ne renvoie que les
# CTE à compléter (sortie courte), le reste de la requête n'est jamais réécrit.
# ---------------------------------------------------------------------------

def _scan_parens(sql: str, start: int) -> int:
    """Position de la parenthèse fermante associée à sql[start] == '('.
    Ignore chaînes, noms `…` et commentaires -- ."""
    depth, i, n = 0, start, len(sql)
    while i < n:
        ch = sql[i]
        if sql.startswith("--", i):
            j = sql.find("\n", i)
            i = n if j == -1 else j
            continue
        if ch in "'\"`":
            j = i + 1
            while j < n and sql[j] != ch:
                j += 2 if sql[j] == "\\" else 1
            i = j + 1
            continue
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                return i
        i += 1
    return -1


def ctes_of(sql: str) -> list[tuple[str, int, int]]:
    """CTE d'une requête ou d'une réponse : (nom, début du corps, fin)."""
    out, pos = [], 0
    for m in re.finditer(r"\b([A-Za-z_]\w*)\s+AS\s*\(", sql, re.I):
        if m.start() < pos or m.group(1).upper() in ("CAST", "TRY_CAST"):
            continue
        open_ = m.end() - 1
        close = _scan_parens(sql, open_)
        if close == -1:
            break
        # une CTE est suivie de « , » ou du SELECT final, pas d'un autre AS
        out.append((m.group(1), open_ + 1, close))
        pos = close
    return out


def todo_ctes(draft: str) -> list[str]:
    return [name for name, a, b in ctes_of(draft)
            if "TODO(model)" in draft[a:b]]


def splice(draft: str, answer: str) -> tuple[str, list[str]]:
    """Remplace le corps des CTE TODO(model) du brouillon par ceux de la
    réponse ; les CTE nouvelles sont insérées avant la première CTE TODO.
    Renvoie (requête, CTE TODO restées sans réponse)."""
    todos = todo_ctes(draft)
    given = {name.lower(): answer[a:b] for name, a, b in ctes_of(answer)}
    existing = {name.lower() for name, _, _ in ctes_of(draft)}
    helpers = [(name, answer[a:b]) for name, a, b in ctes_of(answer)
               if name.lower() not in existing]
    sql, missing = draft, []
    for name in todos:
        body = given.get(name.lower())
        if body is None:
            missing.append(name)
            continue
        for n_, a, b in ctes_of(sql):
            if n_ == name:
                sql = sql[:a] + "\n" + body.strip("\n") + "\n" + sql[b:]
                break
    if helpers and todos:
        first = next(a for n_, a, b in ctes_of(sql) if n_ == todos[0])
        head = sql.rfind("\n", 0, sql.rfind(todos[0], 0, first)) + 1
        prev = sql.rfind("\n", 0, head - 1) + 1
        if sql[prev:head].lstrip().startswith("-- l."):
            head = prev                    # garder le commentaire avec sa CTE
        block = "".join(f"-- CTE ajoutée par le modèle\n{n_} AS (\n"
                        f"{body.strip(chr(10))}\n),\n" for n_, body in helpers)
        sql = sql[:head] + block + sql[head:]
    sql = sql.replace("à traduire par le modèle", "traduit par le modèle")
    return sql, missing


def answer_sql(ok: bool, log: str, events: dict):
    """Texte de la réponse, requête extraite, et si elle vient d'une
    tentative d'écriture (outil refusé) plutôt que du texte."""
    answer = events.get("text") if events.get("json") else \
        log.split("\n--- stderr ---\n")[0].strip()
    if not ok:
        return answer, None, False
    sql = extract_sql(answer or "")
    if sql:
        return answer, sql, False
    for content in sorted(events.get("tool_inputs", []), key=len, reverse=True):
        sql = extract_sql(content)
        if sql:
            return answer, sql, True
    return answer, None, False


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


class Calls:
    """Appels au modèle d'une table : un log numéroté par appel."""

    def __init__(self, d: Path, sec: dict, args):
        self.d, self.sec, self.args, self.n = d, sec, args, 0
        self.failures, self.last_note = 0, ""

    def call(self, msg: str, holes: bool = False):
        """-> (ok, texte, sql extrait, réponse coupée, note).
        holes : la réponse attendue est « hN: expression », pas une requête."""
        self.n += 1
        t0 = time.perf_counter()
        ok, log, events = run_opencode(msg, self.args)
        if self.args.dry_run:
            return True, "", None, False, ""
        answer, sql, from_tool = answer_sql(ok, log, events)
        tools = events.get("tools", [])
        cut = (events.get("finish") or "").lower() in ("length", "max_tokens",
                                                         "max-tokens")
        if cut:
            note = (f"réponse COUPÉE : limite de tokens de sortie atteinte "
                    f"({events.get('output_tokens') or '?'} tokens)")
        elif from_tool:
            note = ("requête récupérée dans la tentative d'écriture du modèle "
                    f"({', '.join(tools)}) : il n'a pas répondu en texte")
        elif not ok:
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
        elif holes and not parse_holes(answer):
            note = "aucune ligne « hN: expression » dans la réponse du modèle"
        elif not holes and not sql:
            note = "aucune requête SQL trouvée dans la réponse du modèle"
        else:
            note = ""
        self.last_note = note or self.last_note
        if events.get("errors"):
            note += " | erreurs : " + " ; ".join(events["errors"][:3])
        (self.d / f"model_{self.n}.log").write_text(
            f"# Appel {self.n} — {round(time.perf_counter() - t0)}s, "
            f"code retour {'0' if ok else 'erreur'}\n"
            f"# Commande : {self.args.opencode_bin} run --agent {self.args.agent}"
            f"{' --model ' + self.args.model if self.args.model else ''} <message>\n"
            + (f"# Note     : {note}\n" if note else "")
            + "\n## Message envoyé\n\n" + msg
            + (f"\n\n## Outils tentés\n\n{', '.join(tools)}" if tools else "")
            + "\n\n## Texte de la réponse\n\n" + (answer or "(aucun)")
            + "\n\n## Sortie brute d'opencode\n\n" + log, encoding="utf-8")
        if note:
            print(f"    {self.sec['id']} : {note}")
        if not ok:
            self.failures += 1
        return ok, answer or "", sql, cut, note


HOLE_RETRY = """

# Your previous answer
```sql
{previous}
```

Problems found:
{errors}

Answer again for: {pending} (one line each, `hN: <expression>`)."""

MORE_CONTEXT = """

# More context: the whole table, already translated parts summarised
```sql
{draft}
```"""


def parse_holes(text: str) -> dict[str, str]:
    """Réponse « h1: expr » (une ligne, ou plusieurs jusqu'au trou suivant)."""
    block = extract_sql(text) or text
    out, cur = {}, None
    for line in block.splitlines():
        m = re.match(r"\s*(h\d+)\s*[:=]\s*(.*)$", line)
        if m:
            cur = m.group(1)
            out[cur] = m.group(2)
        elif cur and line.strip() and not line.strip().startswith("```"):
            out[cur] += "\n" + line
    return {k: v.strip().rstrip(";,").strip() for k, v in out.items()
            if v.strip()}


def translator_errors(sec: dict, path: Path, parser) -> list[str]:
    """Erreurs du brouillon AVANT toute réponse du modèle : elles viennent du
    traducteur, on ne les reproche pas au modèle (sinon il s'égare)."""
    errors, _ = check_section(sec, path, parser)
    return [e for e in errors if "TODO(model)" not in e]


def holes_phase(calls: Calls, d: Path, sec: dict, args, parser,
                statement_left: bool) -> tuple[dict, list[str]]:
    """Le modèle traduit les expressions laissées par le traducteur.
    -> (réponses acceptées {id: sql}, problèmes restants)."""
    out = d / "query.sql"
    base = out.read_text(encoding="utf-8")
    prompt = (d / "holes_prompt.md").read_text(encoding="utf-8")
    condensed = (d / "draft_condensed.sql").read_text(encoding="utf-8") \
        if (d / "draft_condensed.sql").exists() else ""
    pending = [h["id"] for h in sec.get("holes", [])]
    answers, errors, previous, more = {}, [], "", False
    baseline = set(translator_errors(sec, out, parser))
    for _ in range(args.retries + 2):         # +1 : niveau de contexte
        msg = prompt + (MORE_CONTEXT.format(draft=condensed) if more else "")
        if previous or errors:
            msg += HOLE_RETRY.format(previous=previous or "(nothing)",
                                     errors="\n".join(f"- {e}" for e in errors)
                                     or "- missing answers",
                                     pending=", ".join(pending))
        ok, text, _, _, _ = calls.call(msg, holes=True)
        if args.dry_run:
            return {}, []
        if not ok:
            errors = ["the previous call failed: answer with plain text"]
            continue
        given = parse_holes(text)
        previous = "\n".join(f"{k}: {v}" for k, v in given.items())
        unknown = [k for k in pending if given.get(k) == "?"]
        tentative = {k: v for k, v in given.items() if k in pending and v != "?"}
        candidate = fill_holes(base, {**answers, **tentative})
        out.write_text(candidate, encoding="utf-8")
        problems, _ = check_section(sec, out, parser)
        # les trous encore sans réponse (et les blocs d'instruction) sont
        # attendus ici : seules les erreurs de syntaxe / d'analyse comptent
        problems = [p for p in problems if "TODO(model)" not in p
                    and p not in baseline
                    and not (statement_left and p.startswith("colonnes de sortie"))]
        if problems:
            out.write_text(fill_holes(base, answers), encoding="utf-8")
            errors = problems
            print(f"    {sec['id']} ✗ expressions : {problems[0][:110]}")
        else:
            answers.update(tentative)
            errors = []
        pending = [k for k in pending if k not in answers]
        if unknown and not more:
            more = True                      # le modèle demande plus de contexte
            print(f"    {sec['id']} : le modèle demande plus de contexte "
                  f"({', '.join(unknown)})")
        if not pending:
            break
    left = [f"expression {k} non traduite" for k in pending] + errors
    return answers, left


def statement_phase(calls: Calls, d: Path, sec: dict, args, parser):
    """Le modèle complète les CTE TODO(model) (instructions entières)."""
    out, prompt_path = d / "query.sql", d / "prompt.md"
    prompt = prompt_path.read_text(encoding="utf-8")
    draft = out.read_text(encoding="utf-8")
    baseline = set(translator_errors(sec, out, parser))
    errors: list[str] = []
    previous = draft
    for attempt in range(1, args.retries + 2):
        msg = prompt
        if attempt > 1:
            msg += RETRY_TEXT.format(
                previous=(previous if previous is not draft
                          else "(no SQL in your answer)"),
                errors="\n".join(f"- {e}" for e in errors),
                allowed=", ".join(sorted(sec.get("views", {}))) or "none")
        msg = shrink(msg)
        if len(msg.encode("utf-8")) > MAX_INLINE:
            return ["prompt trop long pour être envoyé dans le message"]
        ok, _, sql, cut, _ = calls.call(msg)
        if args.dry_run:
            return []
        if not ok:
            errors = ["the previous call failed: answer with the TODO(model) "
                      "CTEs in one ```sql block, without writing any file"]
            continue
        if sql:
            full, missing = splice(draft, sql)
            out.write_text(full, encoding="utf-8")
            (d / f"query.attempt{attempt}.sql").write_text(full, encoding="utf-8")
            previous = sql
            errors, _ = check_section(sec, out, parser)
            errors = [e for e in errors if e not in baseline]
            if missing:
                errors.insert(0, "CTE missing or incomplete in your answer: "
                              + ", ".join(missing)
                              + (" (your answer was cut: be shorter)" if cut else ""))
        elif cut:
            errors = ["your previous answer was cut by the output limit: answer "
                      "ONLY with the TODO(model) CTEs, concisely, no explanation"]
        else:
            errors = ["your previous answer contained no SQL. Do not try to "
                      "write files (disabled): reply with plain text, the "
                      "TODO(model) CTEs in one ```sql block"]
        if not errors:
            return []
        if sql:
            shown = errors[0]
            if shown.startswith("CTE missing"):
                shown = "CTE absente ou incomplète dans la réponse : " + \
                    shown.split(": ", 1)[1]
            print(f"    {sec['id']} ✗ tentative {attempt} : {shown[:120]}")
    out.write_text(splice(draft, previous)[0] if previous is not draft else draft,
                   encoding="utf-8")
    if errors and errors[0].startswith(("your previous answer contained no SQL",
                                        "your previous answer was cut")):
        return [calls.last_note or "le modèle n'a renvoyé aucune requête"]
    return errors


def translate_section(workdir: Path, sec: dict, args, parser=None):
    """Complète une table : d'abord les expressions isolées (holes_prompt.md),
    puis les instructions entières (prompt.md), s'il y en a.
    -> (statut, nombre d'appels, problème, réponses aux expressions)."""
    d = workdir / sec["id"]
    for old in list(d.glob("model_*.log")) + list(d.glob("query.attempt*.sql")):
        old.unlink(missing_ok=True)
    calls = Calls(d, sec, args)
    statement_left = bool(sec.get("statement_todos"))
    answers, problems = {}, []
    if sec.get("holes") and (d / "holes_prompt.md").exists():
        answers, problems = holes_phase(calls, d, sec, args, parser,
                                        statement_left)
    if not args.dry_run and todo_ctes((d / "query.sql").read_text(encoding="utf-8")):
        if (d / "prompt.md").exists():
            problems = statement_phase(calls, d, sec, args, parser)
    if args.dry_run:
        return "dry-run", calls.n, "", {}
    final, _ = check_section(sec, d / "query.sql", parser)
    if not final:
        return "ok", calls.n, "", answers
    if calls.failures == calls.n:
        return "erreur_modele", calls.n, "opencode en échec : voir model_*.log", {}
    if not [e for e in final if "TODO(model)" in e]:
        # tout est complété mais le contrôle refuse : souvent le traducteur
        return "escalade", calls.n, ("contrôle refusé (à vérifier, peut venir du "
                                     "traducteur) : " + " | ".join(final))[:500], answers
    return "escalade", calls.n, " | ".join(problems or final)[:500], answers


def selftest(args, quiet: bool = False) -> int:
    """Envoie une question triviale à l'agent et montre ce qui revient.
    quiet : n'affiche rien si tout va bien (contrôle avant traduction)."""
    msg = ("Reply with exactly this text and nothing else, without using any "
           "tool:\n```sql\nSELECT 1 AS ok\n```")
    t0 = time.perf_counter()
    ok, raw, events = run_opencode(msg, args)
    sec = round(time.perf_counter() - t0)
    answer, sql, from_tool = answer_sql(ok, raw, events)
    if quiet and sql:
        return 0
    if quiet:
        print("\n⚠ Le modèle ne répond pas correctement à une question simple :")
    print(f"code retour      : {'0' if ok else 'ERREUR'} ({sec}s)")
    print(f"format json      : {'oui' if events.get('json') else 'non'}")
    print(f"outils tentés    : {', '.join(events.get('tools', [])) or 'aucun'}")
    print(f"raisonnement seul: {'oui' if events.get('reasoning') and not answer else 'non'}")
    print(f"texte reçu       : {answer[:300] if answer else '(aucun)'}")
    print(f"SQL extrait      : {sql or '(aucun)'}"
          + ("  (repris de sa tentative d'écriture)" if from_tool else ""))
    if not sql:
        print("\n--- sortie brute ---\n" + raw[-3000:])
    return 0 if sql else 1


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

    cache_path = args.workdir.parent / "expressions.json"
    cache = load_cache(args.workdir.parent)

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
            sec, (status, attempts, problem, answers), secs = fut.result()
            print(f"   {sec['id']} : {status} en {secs}s")
            if args.dry_run:
                continue
            sec["status"] = status
            if status == "ok" and answers:
                # réponses du modèle aux expressions : en cache, NON vérifiées
                # tant que la comparaison avec Qlik n'a pas été identique
                by_id = {h["id"]: h for h in sec.get("holes", [])}
                sec["hole_answers"] = {hole_key(by_id[k]): v
                                       for k, v in answers.items() if k in by_id}
                for key, sql in sec["hole_answers"].items():
                    entry = cache.setdefault(key, {"sql": sql, "verified": False,
                                                   "tables": []})
                    if entry["sql"] != sql and not entry["verified"]:
                        entry["sql"] = sql
                    ref = f"{manifest['script']}/{sec['id']}"
                    if ref not in entry["tables"]:
                        entry["tables"].append(ref)
                cache_path.write_text(json.dumps(cache, indent=2,
                                                 ensure_ascii=False),
                                      encoding="utf-8")
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
