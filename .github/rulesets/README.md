# Rulesets

`state-version-approval.json` is a repository ruleset in the shape GitHub's import accepts. It is
the enforcement half of code ownership: [CODEOWNERS](../CODEOWNERS) names *who* owns a path, this
says *how many* of them have to approve. The two are complementary by design — GitHub's
[required reviewer rule](https://github.blog/changelog/2026-02-17-required-reviewer-rule-is-now-generally-available/)
went generally available in February 2026 and is documented as augmenting CODEOWNERS, not replacing
it, because [CODEOWNERS has no way to express a count](https://docs.github.com/en/repositories/managing-your-repositorys-settings-and-features/customizing-your-repository/about-code-owners).

**It is not in effect.** The repository's only live ruleset is `master` (id 2247702), which targets
the default branch and has an empty `required_reviewers`. Nothing currently enforces what is written
here — this file is the intent, not the state.

## TODO(settings) — required before any of this takes effect

- [ ] Create `@datazip-inc/state-version-owners` and `@datazip-inc/olake-admins` with **write**
      access. A team that does not exist, or that has read-only access, resolves to nobody and its
      rule is dropped silently rather than blocking.
- [ ] Replace both `"reviewer": { "id": 0 }` placeholders with the real team ids:
      `gh api orgs/datazip-inc/teams/<slug> --jq .id`. Rulesets identify teams by numeric id, and
      GitHub assigns that at creation — it cannot be chosen or predicted.
- [ ] Import the file, either through Settings → Rules → Rulesets → New ruleset → Import, or
      `gh api -X POST repos/datazip-inc/olake/rulesets --input .github/rulesets/state-version-approval.json`
- [ ] Reconcile with the live `master` ruleset. It carries the deletion, non-fast-forward and
      signature rules this file does not, and both would apply to master. Rulesets stack to the
      strictest, so keeping both is safe — but it leaves two places to read before you know what
      protects a branch.

## Keeping it honest

Editing a ruleset in the UI does not update this file. Re-export after any change
(`gh api repos/datazip-inc/olake/rulesets/<id>`), or the file stops being a record of what is
enforced and becomes a description of what someone once intended.
