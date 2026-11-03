# Security Policy

## Supported versions

This project is a portfolio/reference implementation. Security fixes are applied to the `main` branch only.

## Reporting a vulnerability

Do not open a public issue for a security problem. Use GitHub's private vulnerability reporting for the repository, or contact the maintainer through the profile at https://github.com/aya-zid.

Please include:

- A description of the issue and its impact.
- Steps to reproduce.
- Affected files or endpoints.
- Any suggested fix.

You can expect an acknowledgement within a few days.

## Scope

In scope:

- Authentication or authorization bypasses (note: the project has no auth layer; adding one is a known gap).
- Injection vulnerabilities in SQL, HTTP or event handling.
- Secret exposure.
- Unsafe deserialization.
- Container or Kubernetes manifest hardening issues.
- Idempotency or payment logic that can cause duplicate charges or stock corruption.

Out of scope:

- Denial of service against a local development stack.
- Missing production features that are documented as out of scope (TLS, auth, secret manager).
- Vulnerabilities in third-party images that are already fixed upstream; open a Dependabot PR instead.

## Secret handling

The repository must never contain real credentials. `.env.example` and `deploy/k8s/secret.yaml` contain development placeholders only. If you accidentally commit a secret, rotate it immediately and rewrite the history before publishing.

The GitHub token used to push this repository must never be stored in `.git/config`, a script, a commit or an issue. Rotate it if it was shared in plain text.
