"""Fait completer par le modele ce que prepare.py n'a pas su traduire.

Ne passent ici que les tables au statut "todo" : leur query.sql existe
deja, il n'y manque que les trous TODO(model). Pour chaque table :
  1. d'abord les expressions isolees (holes_prompt.md), puis les instructions
     entieres (prompt.md) s'il en reste, chaque fois avec
     opencode run --agent translator dans une session neuve
  2. check.py sur la requete completee
  3. en cas d'erreur, une seule relance, avec la reponse precedente et la
     liste exacte des erreurs
  4. mise a jour du statut dans le manifest et dans journal.csv

Relancer la commande reprend la ou on en etait : seules les sections
"todo" et "erreur_modele" sont retraitees. Une table en "escalade"
n'est jamais retraduite d'office, pour ne pas ecraser une correction faite a
la main (--retry-escalated si on le veut vraiment). Les "deterministic" ne
voient jamais le modele.

Le prompt part dans le message lui-meme et le modele repond en texte : il
n'a besoin ni d'outil ni de fichier, et ses outils sont d'ailleurs tous
coupes. Pour voir ce qu'il a fait : work/<script>/<table>/model_<n>.log
(message et reponse brute) et query.attempt<n>.sql.

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
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))
import journal  # noqa: E402
from check import check_section, get_parser  # noqa: E402
from conf import work_dir  # noqa: E402
from prepare import fill_holes, hole_key, load_cache  # noqa: E402

# Le prompt est passe en argument d'opencode run, pas dans un fichier, donc le
# modele n'a rien a lire et aucune permission a demander. Limite : un argument
# de ligne de commande ne depasse pas 128 Kio sous Linux, d'ou une marge.
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
    """Recupere la requete dans la reponse du modele : le plus long bloc
    ```sql```, ou sinon tout le texte a partir de la premiere ligne qui
    ressemble a du SQL."""
    text = text.split("\n--- stderr ---\n")[0]
    blocks = re.findall(r"```(?:sql)?\s*\n(.*?)```", text, re.S | re.I)
    blocks = [b for b in blocks if re.search(r"\bSELECT\b", b, re.I)]
    if blocks:
        return max(blocks, key=len)
    # pas de balises ```, on part de la premiere ligne SQL
    m = re.search(r"^\s*(--|WITH\b|SELECT\b)", text, re.M | re.I)
    if m and re.search(r"\bSELECT\b", text[m.start() :], re.I):
        return text[m.start() :].strip()
    return None


def short_error(error: str) -> str:
    """Raccourcit une erreur avant de la renvoyer au modele. Sur une table
    large, Spark ajoute a "colonne introuvable" la liste de toutes les
    colonnes ("Did you mean one of the following? [...]"), soit des
    milliers de caracteres."""
    error = re.sub(
        r"(Did you mean one of the following\?\s*\[)([^\]]{200,})\]",
        lambda m: m.group(1) + m.group(2)[:200] + "...]",
        error,
    )
    return error if len(error) <= 600 else error[:600] + "..."


def shrink(msg: str) -> str:
    """Raccourcit un message trop long en enlevant les skills."""
    for marker in ("\n# Skill: ",):
        if len(msg.encode("utf-8")) <= MAX_INLINE:
            break
        head, sep, tail = msg.partition(marker)
        if sep:
            # on coupe la section Skill jusqu'a la section suivante
            rest = re.split(r"\n# (?!Skill: )(?=\S)", tail, maxsplit=1)
            msg = head + ("\n# " + rest[1] if len(rest) > 1 else "")
    return msg


def parse_events(stdout: str) -> dict:
    """Lit la sortie de "opencode run --format json", un evenement JSON
    par ligne : texte final, outils tentes, raisonnement, erreurs.

    Le format change d'une version d'opencode a l'autre, donc on lit large
    et on ignore ce qu'on ne reconnait pas."""
    found: dict[str, Any] = {
        "text": [],
        "tools": [],
        "reasoning": False,
        "errors": [],
        "json": False,
        "tool_inputs": [],
        "finish": None,
        "output_tokens": None,
    }

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
                # ce que le modele a essaye d'ecrire via write / edit (refuses)
                inp = state.get("input") if isinstance(state, dict) else None
                if isinstance(inp, dict):
                    for key in (
                        "content",
                        "newString",
                        "new_string",
                        "text",
                        "patch",
                        "code",
                    ):
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
# Greffe des CTE du modele dans le brouillon. Le modele ne renvoie que les CTE
# a completer, ce qui garde ses reponses courtes, et on ne lui laisse jamais
# reecrire le reste de la requete.
# ---------------------------------------------------------------------------


def _scan_parens(sql: str, start: int) -> int:
    """Renvoie la position de la parenthese qui ferme sql[start] == '('.
    Les chaines, les noms `...` et les commentaires -- sont sautes."""
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
    """Liste les CTE d'une requete (ou d'une reponse) : (nom, debut du
    corps, fin)."""
    out, pos = [], 0
    for m in re.finditer(r"\b([A-Za-z_]\w*)\s+AS\s*\(", sql, re.I):
        if m.start() < pos or m.group(1).upper() in ("CAST", "TRY_CAST"):
            continue
        open_ = m.end() - 1
        close = _scan_parens(sql, open_)
        if close == -1:
            break
        # apres une CTE on trouve "," ou le SELECT final, jamais un autre AS
        out.append((m.group(1), open_ + 1, close))
        pos = close
    return out


def todo_ctes(draft: str) -> list[str]:
    return [name for name, a, b in ctes_of(draft) if "TODO(model)" in draft[a:b]]


def splice(draft: str, answer: str) -> tuple[str, list[str]]:
    """Remplace dans le brouillon le corps des CTE TODO(model) par celui
    donne dans la reponse. Les CTE que le modele a ajoutees sont inserees
    avant la premiere CTE TODO.

    Renvoie (requete, CTE TODO restees sans reponse)."""
    todos = todo_ctes(draft)
    given = {name.lower(): answer[a:b] for name, a, b in ctes_of(answer)}
    existing = {name.lower() for name, _, _ in ctes_of(draft)}
    helpers = [
        (name, answer[a:b])
        for name, a, b in ctes_of(answer)
        if name.lower() not in existing
    ]
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
            # on garde le "-- l.<n>" avec sa CTE
            head = prev
        block = "".join(
            f"-- CTE ajoutee par le modele\n{n_} AS (\n{body.strip(chr(10))}\n),\n"
            for n_, body in helpers
        )
        sql = sql[:head] + block + sql[head:]
    sql = sql.replace("a traduire par le modele", "traduit par le modele")
    return sql, missing


def answer_sql(ok: bool, log: str, events: dict):
    """Renvoie le texte de la reponse, la requete extraite, et un booleen
    qui dit si la requete vient d'une tentative d'ecriture de fichier (outil
    refuse) plutot que du texte."""
    answer = (
        events.get("text")
        if events.get("json")
        else log.split("\n--- stderr ---\n")[0].strip()
    )
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
    """Fait un appel au modele. Renvoie (succes, sortie brute, evenements)."""
    cmd = [args.opencode_bin, "run", "--agent", args.agent]
    if args.model:
        cmd += ["--model", args.model]
    if not args.text_output:
        cmd += ["--format", "json"]
    cmd.append(message)
    if args.dry_run:
        print("    $", " ".join(cmd[:-1]), repr(message[:80]) + "...")
        return True, "", {}
    try:
        res = subprocess.run(cmd, capture_output=True, text=True, timeout=args.timeout)
    except subprocess.TimeoutExpired:
        return False, f"timeout apres {args.timeout}s", {}
    except OSError as exc:
        # "Argument list too long" : le message ne passe pas en argument.
        # On le dit clairement au lieu de planter tout le script.
        return (
            False,
            (
                f"opencode n'a pas pu etre lance ({exc}) : message de "
                f"{len(message.encode('utf-8')) // 1024} Ko"
            ),
            {},
        )
    if (
        res.returncode
        and not args.text_output
        and re.search(
            r"format|unknown (option|argument)|unrecognized", res.stderr, re.I
        )
    ):
        # vieil opencode sans --format json : on lit la sortie texte
        args.text_output = True
        print("    (opencode sans --format json : passage en sortie texte)")
        return run_opencode(message, args)
    raw = res.stdout + ("\n--- stderr ---\n" + res.stderr if res.stderr.strip() else "")
    events = parse_events(res.stdout) if not args.text_output else {}
    return res.returncode == 0, raw, events


class Calls:
    """Les appels au modele pour une table, avec un log numerote par appel."""

    def __init__(self, d: Path, sec: dict, args):
        self.d, self.sec, self.args, self.n = d, sec, args, 0
        self.failures, self.last_note = 0, ""

    def call(self, msg: str, holes: bool = False):
        """Renvoie (ok, texte, sql extrait, reponse coupee, note).

        Avec holes=True on attend des lignes "hN: expression", pas une
        requete."""
        self.n += 1
        t0 = time.perf_counter()
        ok, log, events = run_opencode(msg, self.args)
        if self.args.dry_run:
            return True, "", None, False, ""
        answer, sql, from_tool = answer_sql(ok, log, events)
        tools = events.get("tools", [])
        cut = (events.get("finish") or "").lower() in (
            "length",
            "max_tokens",
            "max-tokens",
        )
        if cut:
            note = (
                f"reponse COUPEE : limite de tokens de sortie atteinte "
                f"({events.get('output_tokens') or '?'} tokens)"
            )
        elif from_tool:
            note = (
                "requete recuperee dans la tentative d'ecriture du modele "
                f"({', '.join(tools)}) : il n'a pas repondu en texte"
            )
        elif not ok:
            note = (
                "opencode a echoue (code retour non nul) : voir la fin "
                "du log, partie stderr"
            )
        elif not answer and tools:
            note = (
                f"le modele a tente d'utiliser un outil ({', '.join(tools)}) "
                "au lieu de repondre : outil refuse, pas de texte"
            )
        elif not answer and events.get("reasoning"):
            note = (
                "le modele n'a produit que du raisonnement, pas de reponse "
                "(limite de tokens ?)"
            )
        elif not answer:
            note = "reponse VIDE du modele (rien sur stdout, voir stderr)"
        elif holes and not parse_holes(answer):
            note = "aucune ligne 'hN: expression' dans la reponse du modele"
        elif not holes and not sql:
            note = "aucune requete SQL trouvee dans la reponse du modele"
        else:
            note = ""
        self.last_note = note or self.last_note
        if events.get("errors"):
            note += " | erreurs : " + " ; ".join(events["errors"][:3])
        (self.d / f"model_{self.n}.log").write_text(
            f"# Appel {self.n} - {round(time.perf_counter() - t0)}s, "
            f"code retour {'0' if ok else 'erreur'}\n"
            f"# Commande : {self.args.opencode_bin} run --agent {self.args.agent}"
            f"{' --model ' + self.args.model if self.args.model else ''} <message>\n"
            + (f"# Note     : {note}\n" if note else "")
            + "\n## Message envoye\n\n"
            + msg
            + (f"\n\n## Outils tentes\n\n{', '.join(tools)}" if tools else "")
            + "\n\n## Texte de la reponse\n\n"
            + (answer or "(aucun)")
            + "\n\n## Sortie brute d'opencode\n\n"
            + log,
            encoding="utf-8",
        )
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
    """Lit une reponse "h1: expr" (sur une ligne, ou sur plusieurs
    jusqu'au trou suivant)."""
    block = extract_sql(text) or text
    out, cur = {}, None
    for line in block.splitlines():
        m = re.match(r"\s*(h\d+)\s*[:=]\s*(.*)$", line)
        if m:
            cur = m.group(1)
            out[cur] = m.group(2)
        elif cur and line.strip() and not line.strip().startswith("```"):
            out[cur] += "\n" + line
    return {k: v.strip().rstrip(";,").strip() for k, v in out.items() if v.strip()}


def translator_errors(sec: dict, path: Path, parser) -> list[str]:
    """Erreurs deja presentes dans le brouillon, avant que le modele n'y
    touche. Elles viennent du traducteur : si on les lui renvoyait, il
    essaierait de les corriger et partirait dans tous les sens."""
    errors, _ = check_section(sec, path, parser)
    return [e for e in errors if "TODO(model)" not in e]


def holes_phase(
    calls: Calls, d: Path, sec: dict, args, parser, statement_left: bool
) -> tuple[dict, list[str]]:
    """Le modele traduit les expressions que le traducteur a laissees.
    Renvoie (reponses acceptees {id: sql}, problemes restants)."""
    out = d / "query.sql"
    base = out.read_text(encoding="utf-8")
    prompt = (d / "holes_prompt.md").read_text(encoding="utf-8")
    condensed = (
        (d / "draft_condensed.sql").read_text(encoding="utf-8")
        if (d / "draft_condensed.sql").exists()
        else ""
    )
    pending = [h["id"] for h in sec.get("holes", [])]
    answers: dict[str, str] = {}
    errors: list[str] = []
    previous, more = "", False
    baseline = set(translator_errors(sec, out, parser))
    # +1 pour la passe avec plus de contexte
    for _ in range(args.retries + 2):
        msg = prompt + (MORE_CONTEXT.format(draft=condensed) if more else "")
        if more and len(msg.encode("utf-8")) > MAX_INLINE:
            msg = prompt  # le contexte en plus ne tient pas : sans lui
        if previous or errors:
            msg += HOLE_RETRY.format(
                previous=previous or "(nothing)",
                errors="\n".join(f"- {short_error(e)}" for e in errors)
                or "- missing answers",
                pending=", ".join(pending),
            )
        if len(msg.encode("utf-8")) > MAX_INLINE:
            return answers, [
                f"prompt des expressions trop long "
                f"({len(msg.encode('utf-8')) // 1024} Ko)"
            ]
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
        # Les trous pas encore remplis et les blocs d'instruction sont normaux
        # a ce stade, seules les erreurs de syntaxe ou d'analyse nous
        # interessent.
        problems = [
            p
            for p in problems
            if "TODO(model)" not in p
            and p not in baseline
            and not (statement_left and p.startswith("colonnes de sortie"))
        ]
        if problems:
            out.write_text(fill_holes(base, answers), encoding="utf-8")
            errors = problems
            print(f"    {sec['id']} KO expressions : {problems[0][:110]}")
        else:
            answers.update(tentative)
            errors = []
        pending = [k for k in pending if k not in answers]
        if unknown and not more:
            # il a repondu "?", il veut plus de contexte
            more = True
            print(
                f"    {sec['id']} : le modele demande plus de contexte "
                f"({', '.join(unknown)})"
            )
        if not pending:
            break
    left = [f"expression {k} non traduite" for k in pending] + errors
    return answers, left


def statement_phase(calls: Calls, d: Path, sec: dict, args, parser):
    """Le modele complete les CTE TODO(model), c'est-a-dire des
    instructions entieres."""
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
                previous=(
                    previous if previous is not draft else "(no SQL in your answer)"
                ),
                errors="\n".join(f"- {short_error(e)}" for e in errors),
                allowed=", ".join(sorted(sec.get("views", {}))) or "none",
            )
        msg = shrink(msg)
        if len(msg.encode("utf-8")) > MAX_INLINE:
            return ["prompt trop long pour etre envoye dans le message"]
        ok, _, sql, cut, _ = calls.call(msg)
        if args.dry_run:
            return []
        if not ok:
            errors = [
                "the previous call failed: answer with the TODO(model) "
                "CTEs in one ```sql block, without writing any file"
            ]
            continue
        if sql:
            full, missing = splice(draft, sql)
            out.write_text(full, encoding="utf-8")
            (d / f"query.attempt{attempt}.sql").write_text(full, encoding="utf-8")
            previous = sql
            errors, _ = check_section(sec, out, parser)
            errors = [e for e in errors if e not in baseline]
            if missing:
                errors.insert(
                    0,
                    "CTE missing or incomplete in your answer: "
                    + ", ".join(missing)
                    + (" (your answer was cut: be shorter)" if cut else ""),
                )
        elif cut:
            errors = [
                "your previous answer was cut by the output limit: answer "
                "ONLY with the TODO(model) CTEs, concisely, no explanation"
            ]
        else:
            errors = [
                "your previous answer contained no SQL. Do not try to "
                "write files (disabled): reply with plain text, the "
                "TODO(model) CTEs in one ```sql block"
            ]
        if not errors:
            return []
        if sql:
            shown = errors[0]
            if shown.startswith("CTE missing"):
                shown = (
                    "CTE absente ou incomplete dans la reponse : "
                    + shown.split(": ", 1)[1]
                )
            print(f"    {sec['id']} KO tentative {attempt} : {shown[:120]}")
    out.write_text(
        splice(draft, previous)[0] if previous is not draft else draft, encoding="utf-8"
    )
    if errors and errors[0].startswith(
        ("your previous answer contained no SQL", "your previous answer was cut")
    ):
        return [calls.last_note or "le modele n'a renvoye aucune requete"]
    return errors


def translate_section(workdir: Path, sec: dict, args, parser=None):
    """Complete une table : les expressions isolees d'abord
    (holes_prompt.md), puis les instructions entieres (prompt.md) s'il y en
    a. Renvoie (statut, nombre d'appels, probleme, reponses aux
    expressions)."""
    d = workdir / sec["id"]
    for old in list(d.glob("model_*.log")) + list(d.glob("query.attempt*.sql")):
        old.unlink(missing_ok=True)
    calls = Calls(d, sec, args)
    statement_left = bool(sec.get("statement_todos"))
    answers: dict[str, str] = {}
    problems: list[str] = []
    if sec.get("holes") and (d / "holes_prompt.md").exists():
        answers, problems = holes_phase(calls, d, sec, args, parser, statement_left)
    if not args.dry_run and todo_ctes((d / "query.sql").read_text(encoding="utf-8")):
        if (d / "prompt.md").exists():
            problems = statement_phase(calls, d, sec, args, parser)
    if args.dry_run:
        return "dry-run", calls.n, "", {}
    final, _ = check_section(sec, d / "query.sql", parser)
    if not final:
        return "ok", calls.n, "", answers
    if calls.failures == calls.n:
        return "erreur_modele", calls.n, "opencode en echec : voir model_*.log", {}
    if not [e for e in final if "TODO(model)" in e]:
        # Plus de trou mais le controle refuse : le probleme vient souvent du
        # traducteur, pas du modele.
        return (
            "escalade",
            calls.n,
            (
                "controle refuse (a verifier, peut venir du "
                "traducteur) : " + " | ".join(final)
            )[:500],
            answers,
        )
    return "escalade", calls.n, " | ".join(problems or final)[:500], answers


def selftest(args, quiet: bool = False) -> int:
    """Pose une question bete a l'agent et affiche ce qui revient.
    Avec quiet=True, n'affiche rien si tout va bien (c'est le test lance
    avant chaque traduction)."""
    msg = (
        "Reply with exactly this text and nothing else, without using any "
        "tool:\n```sql\nSELECT 1 AS ok\n```"
    )
    t0 = time.perf_counter()
    ok, raw, events = run_opencode(msg, args)
    sec = round(time.perf_counter() - t0)
    answer, sql, from_tool = answer_sql(ok, raw, events)
    if quiet and sql:
        return 0
    if quiet:
        print("\n[!] Le modele ne repond pas correctement a une question simple :")
    print(f"code retour      : {'0' if ok else 'ERREUR'} ({sec}s)")
    print(f"format json      : {'oui' if events.get('json') else 'non'}")
    print(f"outils tentes    : {', '.join(events.get('tools', [])) or 'aucun'}")
    print(
        f"raisonnement seul: {'oui' if events.get('reasoning') and not answer else 'non'}"
    )
    print(f"texte recu       : {answer[:300] if answer else '(aucun)'}")
    print(
        f"SQL extrait      : {sql or '(aucun)'}"
        + ("  (repris de sa tentative d'ecriture)" if from_tool else "")
    )
    if not sql:
        print("\n--- sortie brute ---\n" + raw[-3000:])
    return 0 if sql else 1


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument(
        "workdir", type=Path, nargs="?", default=Path("work"), help="work/<script>"
    )
    p.add_argument("--only", help="id d'une section")
    p.add_argument("--limit", type=int, help="nombre max de sections")
    p.add_argument(
        "--force", action="store_true", help="retraduire meme les sections deja ok"
    )
    p.add_argument(
        "--retry-escalated",
        action="store_true",
        help="retraduire aussi les sections en escalade (ecrase leur query.sql)",
    )
    p.add_argument("--retries", type=int, default=1)
    p.add_argument(
        "--jobs",
        type=int,
        default=1,
        help="tables traduites en parallele (selon ton serveur)",
    )
    p.add_argument("--agent", default="translator")
    p.add_argument("--model", help="provider/model")
    p.add_argument("--timeout", type=int, default=600)
    p.add_argument("--opencode-bin", default="opencode")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument(
        "--text-output",
        action="store_true",
        help="ne pas utiliser --format json (anciennes versions)",
    )
    p.add_argument(
        "--selftest",
        action="store_true",
        help="verifier que le modele repond bien, sans rien traduire",
    )
    p.add_argument(
        "--skip-selftest",
        action="store_true",
        help="ne pas verifier le modele avant de traduire",
    )
    args = p.parse_args(argv)

    if args.selftest:
        return selftest(args)
    args.workdir = work_dir(args.workdir)

    mpath = args.workdir / "manifest.json"
    manifest = json.loads(mpath.read_text(encoding="utf-8"))
    journal_path = args.workdir.parent / "journal.csv"
    todo = [
        s
        for s in manifest["sections"]
        if (args.only is None or s["id"] == args.only)
        and (
            s["status"] in ("todo", "erreur_modele")
            or (args.retry_escalated and s["status"] == "escalade")
            or (args.force and s["status"] == "ok")
        )
    ]
    if args.limit:
        todo = todo[: args.limit]
    print(f"{len(todo)} table(s) a completer par le modele")
    if todo and not args.dry_run and not shutil.which(args.opencode_bin):
        print(f"'{args.opencode_bin}' introuvable dans le PATH")
        return 1

    if todo and not args.dry_run and not args.skip_selftest:
        if selftest(args, quiet=True):
            print(
                "\nTraduction arretee : corrige d'abord ce probleme "
                "(modele, agent, permissions), puis relance.\n"
                "Test seul : python tools/translate.py --selftest"
            )
            return 1

    # un seul parseur Spark pour tous les threads, une erreur de syntaxe
    # declenche la relance
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
                # On met en cache les reponses du modele aux expressions, mais
                # marquees non verifiees tant qu'une comparaison avec Qlik ne
                # les a pas confirmees.
                by_id = {h["id"]: h for h in sec.get("holes", [])}
                sec["hole_answers"] = {
                    hole_key(by_id[k]): v for k, v in answers.items() if k in by_id
                }
                for key, sql in sec["hole_answers"].items():
                    entry = cache.setdefault(
                        key, {"sql": sql, "verified": False, "tables": []}
                    )
                    if entry["sql"] != sql and not entry["verified"]:
                        entry["sql"] = sql
                    ref = f"{manifest['script']}/{sec['id']}"
                    if ref not in entry["tables"]:
                        entry["tables"].append(ref)
                cache_path.write_text(
                    json.dumps(cache, indent=2, ensure_ascii=False), encoding="utf-8"
                )
            mpath.write_text(
                json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8"
            )
            journal.update(
                journal_path,
                {
                    "date": dt.datetime.now().strftime("%Y-%m-%d %H:%M"),
                    "script": manifest["script"],
                    "section": sec["id"],
                    "status": status,
                    "attempts": attempts,
                    "seconds": secs,
                    "problem": problem,
                },
            )

    counts: dict[str, int] = {}
    for s in manifest["sections"]:
        counts[s["status"]] = counts.get(s["status"], 0) + 1
    print(f"\nTermine en {round(time.perf_counter() - t_all)}s : {counts}")
    print(f"Journal : {journal_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
