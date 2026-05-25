# Security Policy

## Reporting a Vulnerability

Do not report suspected vulnerabilities through a public GitHub issue.

Use GitHub private vulnerability reporting for this repository if it is enabled. If private reporting is not yet available, use the private maintainer contact channel listed in the repository settings before disclosing details publicly.

Please include:

- the affected version, commit, or file path
- a short description of impact
- clear reproduction steps
- any logs or packet captures with secrets and personal paths removed

## Disclosure Expectations

- Please allow the maintainers reasonable time to reproduce and remediate the issue before public disclosure.
- Coordinated disclosure is preferred for issues involving authentication, transport security, persistence safety, or denial-of-service behavior.

## Scope Notes

- Public reports must not include real broker hosts, credentials, certificates, account identifiers, or workstation-specific paths.
- Findings that depend only on private operational infrastructure may be out of scope for the public library release, but the underlying reusable library issue should still be reported.
