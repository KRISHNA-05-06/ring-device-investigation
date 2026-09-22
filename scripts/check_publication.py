"""Inspect tracked and untracked non-ignored files without printing secret values."""
from pathlib import Path
import re
import subprocess

ROOT = Path(__file__).resolve().parents[1]
RULES = {
    'AWS access key': re.compile(r'\b(?:AKIA|ASIA)[A-Z0-9]{16}\b'),
    'GitHub token': re.compile(r'\b(?:gh[pousr]_[A-Za-z0-9]{30,}|github_pat_[A-Za-z0-9_]{50,})\b'),
    'private key': re.compile(r'-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----'),
    'AWS secret assignment': re.compile(r'''(?im)^\s*aws_secret_access_key\s*=\s*["']?[A-Za-z0-9/+=]{35,}'''),
    'password literal': re.compile(r'''(?im)\b(?:DB_PASSWORD|password)\s*[:=]\s*["'][^"'\n]{8,}["']'''),
}
FORBIDDEN = {'local-only', '.aws', '.venv-aws', '__pycache__', 'sessions', 'node_modules'}


def main():
    output = subprocess.check_output(['git', 'ls-files', '-z', '--cached', '--others', '--exclude-standard'], cwd=ROOT)
    files = sorted(set(name for name in output.decode('utf-8').split('\0') if name))
    # Tracked deletions remain in the index until staged; inspect only present files.
    files = [name for name in files if (ROOT / name).is_file()]
    problems = []
    for name in files:
        path = Path(name)
        if any(part in FORBIDDEN for part in path.parts) or path.suffix.lower() in {'.pdf', '.pem', '.key', '.zip'}:
            problems.append(f'{name}: private/generated file included')
        raw = (ROOT / path).read_bytes()
        try:
            text = raw.decode('utf-8-sig')
        except UnicodeDecodeError:
            continue
        for label, rule in RULES.items():
            for hit in rule.finditer(text):
                problems.append(f'{name}:{text.count(chr(10), 0, hit.start()) + 1}: possible {label}')
    for problem in problems:
        print(problem)
    if problems:
        raise SystemExit('FAIL: review findings before publishing. Values were not printed.')
    print(f'PASS: {len(files)} publishable files inspected; no matches for the configured secret patterns or forbidden paths.')
    print('Scope: pattern checks only. AWS resource identifiers remain; review SECURITY.md and the Git diff.')


if __name__ == '__main__':
    main()
